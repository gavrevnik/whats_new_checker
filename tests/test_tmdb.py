from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import tmdb


class TmdbTests(unittest.TestCase):
    def test_secrets_file_is_parsed_without_execution_and_environment_wins(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "SECRETS").write_text(
                'TMDB_API_KEY = "tmdb-test"\nOMDB_API_KEY = "omdb-test"\nKINOPOISK_API_KEY = "kp-test"\n',
                encoding="utf-8",
            )
            with patch.object(tmdb, "ROOT", Path(directory)), patch.dict(os.environ, {}, clear=True):
                self.assertEqual(tmdb.get_api_key(), ("tmdb-test", "SECRETS"))
                self.assertEqual(tmdb.get_omdb_key(), ("omdb-test", "SECRETS"))
                self.assertEqual(tmdb.get_kinopoisk_key(), ("kp-test", "SECRETS"))
            with patch.object(tmdb, "ROOT", Path(directory)), patch.dict(os.environ, {"TMDB_API_KEY":"env-test"}, clear=True):
                self.assertEqual(tmdb.get_api_key(), ("env-test", "environment"))

    def test_resolve_rejects_ambiguous_search(self) -> None:
        payload = {"results":[
            {"id":1,"title":"Версия A","original_title":"Version A","release_date":"1980-01-01"},
            {"id":2,"title":"Версия B","original_title":"Version B","release_date":"1980-06-01"},
        ]}
        with patch.object(tmdb,"_get",return_value=payload):
            with self.assertRaises(tmdb.TmdbError):
                tmdb.resolve_movie("Unknown","Неизвестный",1980,"key")

    def test_movie_details_maps_interests_and_companion_data(self) -> None:
        payload = {
            "id":42,"title":"Тест","original_title":"Test","release_date":"2025-05-01","runtime":121,
            "vote_average":7.64,"vote_count":500,"imdb_id":"tt42","overview":"Описание","original_language":"en",
            "genres":[{"id":18,"name":"драма"}],
            "credits":{"crew":[{"id":10,"name":"Director","job":"Director"}],"cast":[{"id":20,"name":"Actor","character":"Hero"}]},
        }
        interests = {10:{"role":"director"},20:{"role":"actor"}}
        with patch.object(tmdb,"_get",return_value=payload), patch.object(tmdb,"_interest_index",return_value=interests), patch.object(tmdb,"_get_omdb",return_value={"imdb_rating":8.1,"awards_json":[{"source":"omdb","summary":"2 wins"}]}), patch.object(tmdb,"_get_kinopoisk",return_value={"kinopoisk_id":301,"kinopoisk_rating":8.5}):
            result = tmdb.movie_details(42,"key")
        self.assertEqual(result["imdb_rating"],8.1)
        self.assertIn("Режиссёры: Director",result["key_people"])
        self.assertIn("Актёры: Actor",result["key_people"])
        self.assertEqual(result["awards_json"][0]["summary"],"2 wins")
        self.assertEqual(result["kinopoisk_rating"],8.5)

    def test_kinopoisk_uses_single_title_lookup_and_maps_rating(self) -> None:
        payload = {"items":[{
            "kinopoiskId":301, "imdbId":"tt0133093", "nameRu":"Матрица",
            "nameOriginal":"The Matrix", "year":1999, "ratingKinopoisk":8.5,
        }]}
        with patch.object(tmdb,"get_kinopoisk_key",return_value=("kp-key","test")), \
             patch.object(tmdb,"_request_json",return_value=payload) as request:
            result = tmdb._get_kinopoisk("tt0133093","The Matrix","Матрица",1999)
        url, headers = request.call_args.args
        self.assertIn("keyword=", url)
        self.assertNotIn("imdbId=", url)
        self.assertEqual(headers["X-API-KEY"], "kp-key")
        self.assertEqual(result["kinopoisk_id"], 301)
        self.assertEqual(result["kinopoisk_rating"], 8.5)
        self.assertEqual(result["kinopoisk_link"], "https://www.kinopoisk.ru/film/301/")

    def test_kinopoisk_falls_back_to_title_and_year_in_one_request(self) -> None:
        payload = {"items":[{
            "kinopoiskId":735, "imdbId":None, "nameRu":"Сияние",
            "nameOriginal":"The Shining", "year":1980, "ratingKinopoisk":7.8,
        }]}
        with patch.object(tmdb,"get_kinopoisk_key",return_value=("kp-key","test")), \
             patch.object(tmdb,"_request_json",return_value=payload) as request:
            result = tmdb._get_kinopoisk("","The Shining","Сияние",1980)
        url = request.call_args.args[0]
        self.assertIn("keyword=", url)
        self.assertIn("yearFrom=1979", url)
        self.assertIn("yearTo=1981", url)
        self.assertEqual(result["kinopoisk_id"], 735)

    def test_kinopoisk_matches_title_when_provider_has_no_imdb_mapping(self) -> None:
        payload = {"items":[{
            "kinopoiskId":256408, "imdbId":None, "nameRu":"Древо жизни",
            "nameOriginal":"The Tree of Life", "year":2010, "ratingKinopoisk":6.6,
        }]}
        with patch.object(tmdb,"get_kinopoisk_key",return_value=("kp-key","test")), \
             patch.object(tmdb,"_request_json",return_value=payload):
            result = tmdb._get_kinopoisk("tt0478304","The Tree of Life","Древо жизни",2011)
        self.assertEqual(result["kinopoisk_id"], 256408)
        self.assertEqual(result["kinopoisk_rating"], 6.6)

    def test_kinopoisk_quota_error_becomes_non_fatal_warning(self) -> None:
        with patch.object(tmdb,"get_kinopoisk_key",return_value=("kp-key","test")), \
             patch.object(tmdb,"_request_json",side_effect=tmdb.TmdbError("Provider returned HTTP 402: quota exceeded")):
            result = tmdb._get_kinopoisk("tt0478304","The Tree of Life","Древо жизни",2011)
        self.assertNotIn("kinopoisk_rating", result)
        self.assertEqual(result["provider_warnings"][0]["kind"], "limit")
        self.assertIn("лимит", result["provider_warnings"][0]["message"])

    def test_tmdb_provider_error_remains_fatal_and_is_labeled(self) -> None:
        with patch.object(tmdb,"_request_json",side_effect=tmdb.TmdbError("Provider returned HTTP 429: limit")):
            with self.assertRaisesRegex(tmdb.TmdbError, "^TMDB:"):
                tmdb._get("/movie/1", {"language":"ru-RU"}, "key")

    def test_person_details_uses_canonical_and_russian_alias(self) -> None:
        payload = {
            "id":514, "name":"Jack Nicholson", "also_known_as":["Джек Николсон"],
            "birthday":"1937-04-22", "place_of_birth":"Neptune City", "known_for_department":"Acting",
        }
        with patch.object(tmdb,"_get",return_value=payload):
            result = tmdb.person_details(514,"key")
        self.assertEqual(result["name_original"],"Jack Nicholson")
        self.assertEqual(result["name_ru"],"Джек Николсон")
        self.assertEqual(result["details_json"]["birthday"],"1937-04-22")

    def test_resolve_person_prefers_expected_department_and_popularity(self) -> None:
        payload = {"results":[
            {"id":1,"name":"Ari Aster","known_for_department":"Production","popularity":5},
            {"id":2,"name":"Ari Aster","known_for_department":"Directing","popularity":1},
        ]}
        with patch.object(tmdb,"_get",return_value=payload):
            self.assertEqual(tmdb.resolve_person("Ari Aster","director","key"),2)

    def test_resolve_movie_input_returns_normalized_details(self) -> None:
        details = {"tmdb_id":694, "title_original":"The Shining", "title_ru":"Сияние"}
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb,"resolve_movie",return_value=694) as resolve, \
             patch.object(tmdb,"movie_details",return_value=details) as fetch:
            result = tmdb.resolve_movie_input({"title_ru":"сияние", "year":"1980"})
        resolve.assert_called_once_with("", "сияние", "1980", "key")
        fetch.assert_called_once_with(694, "key")
        self.assertEqual(result, details)

    def test_resolve_movie_input_researches_an_edited_title_instead_of_stale_tmdb_id(self) -> None:
        details = {"tmdb_id":123, "title_original":"Between Worlds", "title_ru":"На границе миров"}
        payload = {
            "title_original":"Old provider title", "title_ru":"На границе миров", "year":"2018",
            "tmdb_id":"999", "search_field":"title_ru",
        }
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb,"resolve_movie",return_value=123) as resolve, \
             patch.object(tmdb,"movie_details",return_value=details):
            result = tmdb.resolve_movie_input(payload)
        resolve.assert_called_once_with("", "На границе миров", "2018", "key")
        self.assertEqual(result["tmdb_id"], 123)

    def test_resolve_person_input_can_use_tmdb_id(self) -> None:
        details = {"tmdb_id":514, "name_original":"Jack Nicholson", "name_ru":"Джек Николсон"}
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb,"person_details",return_value=details) as fetch:
            result = tmdb.resolve_person_input({"role":"actor", "tmdb_id":"514", "name_ru":"джек"})
        fetch.assert_called_once_with(514, "key")
        self.assertEqual(result["role"], "actor")

    def test_refresh_person_updates_only_requested_person(self) -> None:
        targets = [
            {"id":"person-one","name_original":"One","name_ru":"Один","role":"actor","tmdb_id":1},
            {"id":"person-two","name_original":"Two","name_ru":"Два","role":"director","tmdb_id":2},
        ]
        details = {"tmdb_id":2,"name_original":"Two","name_ru":"Два","details_json":{}}
        updated = {**targets[1], **details}
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb.storage,"list_interests",return_value=targets), \
             patch.object(tmdb,"_fetch_person_target",return_value=("person-two",details)) as fetch, \
             patch.object(tmdb.storage,"update_interest_person",return_value=updated) as save:
            result = tmdb.refresh_person("person-two")
        fetch.assert_called_once_with(targets[1], "key")
        save.assert_called_once_with("person-two", details)
        self.assertEqual(result["id"], "person-two")

    def test_refresh_movie_runs_full_tmdb_update_when_imdb_rating_is_missing(self) -> None:
        target = {
            "id":"movie-one", "title_original":"The Matrix", "title_ru":"Матрица",
            "year":1999, "tmdb_id":603, "imdb_id":"tt0133093",
            "imdb_rating":"", "kinopoisk_rating":8.5, "directors":"Lana Wachowski",
            "poster_local_path":"movies/603.jpg",
        }
        details = {**target, "imdb_rating":8.7}
        with patch.object(tmdb.storage,"get_item",return_value=target), \
             patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb,"movie_details",return_value=details) as fetch, \
             patch.object(tmdb,"_get_kinopoisk") as kinopoisk, \
             patch.object(tmdb.storage,"update_movie_from_provider",return_value=details) as save:
            tmdb.refresh_movie("movie-one")
        fetch.assert_called_once_with(603, "key", fetch_kinopoisk=False)
        kinopoisk.assert_not_called()
        save.assert_called_once_with("movie-one", details)

    def test_refresh_movie_calls_only_kinopoisk_when_only_its_rating_is_missing(self) -> None:
        target = {
            "id":"movie-one", "title_original":"The Matrix", "title_ru":"Матрица",
            "year":1999, "tmdb_id":603, "imdb_id":"tt0133093",
            "imdb_rating":8.7, "kinopoisk_rating":"", "directors":"Lana Wachowski",
            "poster_local_path":"movies/603.jpg",
        }
        kp = {"kinopoisk_id":301, "kinopoisk_rating":8.5}
        with patch.object(tmdb.storage,"get_item",return_value=target), \
             patch.object(tmdb.artwork,"is_cached",return_value=True), \
             patch.object(tmdb,"_get_omdb") as imdb, \
             patch.object(tmdb,"_get_kinopoisk",return_value=kp) as kinopoisk, \
             patch.object(tmdb.storage,"update_movie_ratings",return_value={**target,**kp}) as save:
            tmdb.refresh_movie("movie-one")
        imdb.assert_not_called()
        kinopoisk.assert_called_once_with("tt0133093", "The Matrix", "Матрица", 1999)
        save.assert_called_once_with("movie-one", kp)

    def test_refresh_movie_skips_all_providers_when_both_ratings_exist(self) -> None:
        target = {"id":"movie-one", "imdb_rating":8.7, "kinopoisk_rating":8.5, "directors":"Director", "poster_local_path":"movies/1.jpg"}
        with patch.object(tmdb.storage,"get_item",return_value=target), \
             patch.object(tmdb.artwork,"is_cached",return_value=True), \
             patch.object(tmdb,"_refresh_movie_target") as fetch:
            result = tmdb.refresh_movie("movie-one")
        fetch.assert_not_called()
        self.assertEqual(result, target)

    def test_refresh_movie_downloads_only_poster_when_only_local_file_is_missing(self) -> None:
        target = {
            "id":"movie-one", "tmdb_id":603, "imdb_rating":8.7,
            "kinopoisk_rating":8.5, "directors":"Director", "poster_path":"/poster.jpg",
            "poster_url":"https://image.tmdb.org/t/p/w500/poster.jpg", "poster_local_path":"",
        }
        poster = {"poster_path":"/poster.jpg", "poster_url":target["poster_url"], "poster_local_path":"movies/603.jpg"}
        with patch.object(tmdb.storage,"get_item",return_value=target), \
             patch.object(tmdb.artwork,"is_cached",return_value=False), \
             patch.object(tmdb,"_poster_details",return_value=poster) as fetch, \
             patch.object(tmdb,"movie_details") as full, \
             patch.object(tmdb,"_get_kinopoisk") as kinopoisk, \
             patch.object(tmdb.storage,"update_movie_poster",return_value={**target,**poster}) as save:
            tmdb.refresh_movie("movie-one")
        fetch.assert_called_once_with(target)
        full.assert_not_called()
        kinopoisk.assert_not_called()
        save.assert_called_once_with("movie-one", poster)

    def test_bulk_rating_refresh_queues_only_movies_with_missing_rating(self) -> None:
        targets = [
            {"id":"complete", "title_original":"Complete", "imdb_rating":8, "kinopoisk_rating":7, "directors":"D", "poster_local_path":"movies/1.jpg"},
            {"id":"missing-imdb", "title_original":"One", "imdb_rating":"", "kinopoisk_rating":7, "directors":"D", "poster_local_path":"movies/2.jpg"},
            {"id":"missing-kp", "title_original":"Two", "imdb_rating":8, "kinopoisk_rating":"", "directors":"D", "poster_local_path":"movies/3.jpg"},
        ]
        with patch.object(tmdb.storage, "movie_refresh_targets", return_value=targets), \
             patch.object(tmdb.artwork, "is_cached", return_value=True), \
             patch.object(tmdb, "_refresh_movie_target", side_effect=lambda item: item) as fetch:
            result = tmdb.refresh_library()
        self.assertEqual({call.args[0]["id"] for call in fetch.call_args_list}, {"missing-imdb", "missing-kp"})
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["updated"], 2)

    def test_api_recommendations_apply_year_and_translated_genres_before_enrichment(self) -> None:
        credits = {"cast":[
            {"id":1,"title":"Документальный","original_title":"Documentary","release_date":"2024-01-01","genre_ids":[99],"vote_average":9,"vote_count":500},
            {"id":2,"title":"Анимация","original_title":"Animation","release_date":"2023-01-01","genre_ids":[16],"vote_average":9,"vote_count":500},
            {"id":3,"title":"Драма","original_title":"Drama","release_date":"2024-06-01","genre_ids":[18],"vote_average":1,"vote_count":500},
            {"id":4,"title":"Будущее","original_title":"Future","release_date":"2027-01-01","genre_ids":[18],"vote_average":9,"vote_count":500},
        ]}
        details = {"tmdb_id":3,"title_ru":"Драма","title_original":"Drama","year":"2024","duration_minutes":120,"imdb_rating":7,"genres":"драма"}
        interests = lambda _content, role: [{"id":"actor-one","external_id":10,"role":"actor"}] if role == "actor" else []
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb.storage,"list_interests",side_effect=interests), \
             patch.object(tmdb.storage,"known_movie_keys",return_value=(set(),set())), \
             patch.object(tmdb,"_get",return_value=credits), \
             patch.object(tmdb,"movie_details",return_value=details) as enrich:
            result = tmdb.recommend_movies({
                "actor_ids":["actor-one"], "date_from":"2020-01-01", "date_to":"2025-12-31",
                "excluded_genres":["Animation","Documentary"], "min_tmdb_rating":9, "limit":20,
            })
        self.assertEqual([item["tmdb_id"] for item in result], [3])
        enrich.assert_called_once_with(3, "key")

    def test_api_recommendations_filter_by_kinopoisk_rating(self) -> None:
        credits = {"cast":[{"id":3,"title":"Драма","original_title":"Drama","release_date":"2024-06-01","genre_ids":[18],"vote_average":8,"vote_count":500}]}
        details = {"tmdb_id":3,"title_ru":"Драма","title_original":"Drama","year":"2024","duration_minutes":120,"imdb_rating":7,"kinopoisk_rating":6.5,"genres":"драма"}
        interests = lambda _content, role: [{"id":"actor-one","external_id":10,"role":"actor"}] if role == "actor" else []
        with patch.object(tmdb,"get_api_key",return_value=("key","test")), \
             patch.object(tmdb.storage,"list_interests",side_effect=interests), \
             patch.object(tmdb.storage,"known_movie_keys",return_value=(set(),set())), \
             patch.object(tmdb,"_get",return_value=credits), \
             patch.object(tmdb,"movie_details",return_value=details):
            result = tmdb.recommend_movies({"actor_ids":["actor-one"],"min_kinopoisk_rating":7})
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
