from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from app import storage


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "test.sqlite3"
        self.db_patch = patch.object(storage, "DB_PATH", self.db)
        self.db_patch.start()
        storage.initialize_database()
        with storage.connect() as connection:
            connection.execute("INSERT INTO people(id,name_original,name_ru,tmdb_id,active) VALUES ('nolan','Christopher Nolan','Кристофер Нолан',525,1)")
            connection.execute("INSERT INTO interest_roles(person_id,content_type,role) VALUES ('nolan','movie','director')")

    def tearDown(self) -> None:
        self.db_patch.stop()
        self.temp.cleanup()

    def test_add_update_and_relations(self) -> None:
        created = storage.add_item({
            "content_type":"movie", "title_original":"Inception", "title_ru":"Начало", "year":"2010",
            "release_date":"2010-07-16", "status":"backlog", "tmdb_id":"27205", "imdb_id":"tt1375666",
            "directors_data":[{"name":"Christopher Nolan","tmdb_id":525,"role":"director"}],
            "genres_data":[{"id":878,"name":"Фантастика"}], "awards_json":[{"source":"test","summary":"4 Oscars"}],
        })
        self.assertEqual(created["directors"], "Кристофер Нолан (Christopher Nolan)")
        self.assertIn("Режиссёры: Кристофер Нолан (Christopher Nolan)", created["key_people"])
        self.assertEqual(created["genres"], "Фантастика")
        self.assertEqual(json.loads(created["awards_json"])[0]["summary"], "4 Oscars")
        updated = storage.update_item(created["id"], {"status":"consumed","reaction":"like"})
        self.assertEqual(updated["reaction"], "like")
        self.assertTrue(updated["consumed_at"])
        reset = storage.update_item(created["id"], {"reaction":""})
        self.assertEqual(reset["reaction"], "")

    def test_return_to_backlog_clears_reaction(self) -> None:
        created = storage.add_item({"content_type":"movie","title_original":"Memento","title_ru":"Помни","status":"consumed","reaction":"dislike"})
        updated = storage.update_item(created["id"], {"status":"backlog"})
        self.assertEqual(updated["reaction"], "")
        self.assertEqual(updated["consumed_at"], "")

    def test_duplicate_tmdb_or_title_is_rejected(self) -> None:
        storage.add_item({"content_type":"movie","title_original":"The Shining","title_ru":"Сияние","year":"1980","tmdb_id":694})
        with self.assertRaises(storage.StorageError):
            storage.add_item({"content_type":"movie","title_original":"Shining","title_ru":"Сияние!","year":"1980"})
        with self.assertRaises(storage.StorageError):
            storage.add_item({"content_type":"movie","title_original":"Other","title_ru":"Другое","year":"2020","tmdb_id":694})

    def test_schema_integrity(self) -> None:
        with closing(sqlite3.connect(self.db)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"content_items","movies","people","interest_roles","movie_people","genres","content_aliases","trash_entries"} <= tables)
        with closing(sqlite3.connect(self.db)) as connection:
            self.assertIn("raw_json", {row[1] for row in connection.execute("PRAGMA table_info(content_items)")})
            self.assertIn("raw_json", {row[1] for row in connection.execute("PRAGMA table_info(people)")})
            movie_columns = {row[1] for row in connection.execute("PRAGMA table_info(movies)")}
            self.assertIn("kinopoisk_id", movie_columns)
            self.assertIn("kinopoisk_rating", movie_columns)

    def test_people_can_be_added_and_refreshed(self) -> None:
        created = storage.add_interest_person({
            "role":"actor", "name_original":"Jack Nicholson", "name_ru":"Джек Николсон", "tmdb_id":514,
        })
        self.assertEqual(created["tmdb_id"], 514)
        updated = storage.update_interest_person(created["id"], {
            "tmdb_id":514, "name_original":"Jack Nicholson", "name_ru":"Джек Николсон",
            "details_json":{"birthday":"1937-04-22"},
        })
        self.assertIn("1937-04-22", updated["details_json"])

    def test_person_refresh_merges_existing_tmdb_credit_record(self) -> None:
        created = storage.add_interest_person({
            "role":"director", "name_original":"Bennett Miller", "name_ru":"Беннетт Миллер", "tmdb_id":999999,
        })
        with storage.connect() as connection:
            connection.execute(
                "INSERT INTO people(id,name_original,name_ru,tmdb_id,active) VALUES ('provider-copy','Bennett Miller','Беннетт Миллер',5345,0)"
            )
        updated = storage.update_interest_person(created["id"], {
            "tmdb_id":5345, "name_original":"Bennett Miller", "name_ru":"Беннетт Миллер",
        })
        self.assertEqual(updated["tmdb_id"], 5345)
        with storage.connect() as connection:
            self.assertIsNone(connection.execute("SELECT 1 FROM people WHERE id='provider-copy'").fetchone())

    def test_provider_details_and_external_links_are_persisted(self) -> None:
        created = storage.add_item({"content_type":"movie","title_original":"Test","title_ru":"Тест","tmdb_id":123})
        updated = storage.update_movie_from_provider(created["id"], {
            "title_original":"Test", "title_ru":"Тест", "tmdb_id":123, "imdb_id":"tt0123",
            "kinopoisk_id":456, "kinopoisk_rating":7.4,
            "tagline":"A tagline", "imdb_votes":"12,345", "metascore":77, "box_office":"$1,000",
            "cast":"Actor One; Actor Two", "keywords":"memory; dream",
        })
        self.assertEqual(updated["cast"], "Actor One; Actor Two")
        self.assertEqual(updated["imdb_link"], "https://www.imdb.com/title/tt0123/")
        self.assertEqual(updated["kinopoisk_link"], "https://www.kinopoisk.ru/film/456/")
        self.assertEqual(updated["kinopoisk_rating"], 7.4)
        self.assertEqual(updated["metascore"], 77)

        refreshed = storage.update_movie_from_provider(created["id"], {
            "title_original":"Test", "title_ru":"Тест", "tmdb_id":123,
        })
        self.assertEqual(refreshed["kinopoisk_rating"], 7.4)

    def test_movie_raw_input_and_trash_restore(self) -> None:
        raw = {"title_ru":"сияние", "year":"1980"}
        created = storage.add_item({
            "content_type":"movie", "title_original":"The Shining", "title_ru":"Сияние",
            "year":"1980", "tmdb_id":694, "raw_data":raw,
        })
        self.assertEqual(json.loads(created["raw_json"]), raw)
        trashed = storage.trash_entity({"entity_type":"movie", "entity_id":created["id"]})
        self.assertEqual(storage.list_library(), [])
        self.assertEqual(storage.list_trash()[0]["title_original"], "The Shining")
        known_ids, _ = storage.known_movie_keys()
        self.assertIn("694", known_ids)
        storage.restore_trash(trashed["id"])
        self.assertEqual(storage.list_library()[0]["id"], created["id"])
        self.assertEqual(storage.list_trash(), [])

    def test_person_raw_input_and_trash_restore(self) -> None:
        raw = {"role":"actor", "name_ru":"иван иванов"}
        created = storage.add_interest_person({
            "role":"actor", "name_original":"Ivan Ivanov", "name_ru":"Иван Иванов",
            "tmdb_id":987654, "raw_data":raw,
        })
        self.assertEqual(json.loads(created["raw_json"]), raw)
        trashed = storage.trash_entity({"entity_type":"person", "entity_id":created["id"], "role":"actor"})
        self.assertFalse(any(item["id"] == created["id"] for item in storage.list_interests("movie")))
        self.assertEqual(storage.list_trash()[0]["role"], "actor")
        storage.restore_trash(trashed["id"])
        self.assertTrue(any(item["id"] == created["id"] for item in storage.list_interests("movie")))


if __name__ == "__main__":
    unittest.main()
