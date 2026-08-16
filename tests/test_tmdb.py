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
                'TMDB_API_KEY = "tmdb-test"\nOMDB_API_KEY = "omdb-test"\n', encoding="utf-8"
            )
            with patch.object(tmdb, "ROOT", Path(directory)), patch.dict(os.environ, {}, clear=True):
                self.assertEqual(tmdb.get_api_key(), ("tmdb-test", "SECRETS"))
                self.assertEqual(tmdb.get_omdb_key(), ("omdb-test", "SECRETS"))
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
        with patch.object(tmdb,"_get",return_value=payload), patch.object(tmdb,"_interest_index",return_value=interests), patch.object(tmdb,"_get_omdb",return_value={"imdb_rating":8.1,"awards_json":[{"source":"omdb","summary":"2 wins"}]}):
            result = tmdb.movie_details(42,"key")
        self.assertEqual(result["imdb_rating"],8.1)
        self.assertIn("Режиссёры: Director",result["key_people"])
        self.assertIn("Актёры: Actor",result["key_people"])
        self.assertEqual(result["awards_json"][0]["summary"],"2 wins")

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


if __name__ == "__main__":
    unittest.main()
