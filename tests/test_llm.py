from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import llm, storage


class LlmTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "test.sqlite3"
        self.db_patch = patch.object(storage, "DB_PATH", self.db)
        self.db_patch.start()
        storage.initialize_database()
        storage.add_item({
            "title_original":"Arrival", "title_ru":"Прибытие", "year":"2016",
            "status":"consumed", "reaction":"like", "directors":"Denis Villeneuve",
        })
        storage.add_item({
            "title_original":"Dune: Part Two", "title_ru":"Дюна: Часть вторая", "year":"2024",
            "status":"backlog", "directors":"Denis Villeneuve",
        })
        storage.add_interest_person({
            "role":"director", "name_original":"David Fincher", "name_ru":"Дэвид Финчер", "tmdb_id":7467,
        })

    def tearDown(self) -> None:
        self.db_patch.stop()
        self.temp.cleanup()

    def test_build_prompt_contains_filters_and_library_context(self) -> None:
        prompt = llm.build_movie_prompt({
            "prompt":"Хочу мрачный детектив", "min_imdb_rating":"7", "year_from":"2020",
            "year_to":"2026", "min_runtime":"110", "limit":"7",
        })
        self.assertIn("IMDb rating не ниже 7", prompt)
        self.assertIn("от 2020 до 2026", prompt)
        self.assertIn("не меньше 110 минут", prompt)
        self.assertIn("Прибытие (Arrival)", prompt)
        self.assertIn("Дюна: Часть вторая (Dune: Part Two)", prompt)
        self.assertIn("Дэвид Финчер (David Fincher)", prompt)
        self.assertIn("их участие не обязательно", prompt)
        self.assertIn("Хочу мрачный детектив", prompt)
        self.assertIn("Верни 7 наиболее релевантных", prompt)
        self.assertIn("JSON-объект с массивом movies", prompt)

    def test_rejects_inverted_year_range(self) -> None:
        with self.assertRaises(llm.LlmError):
            llm.build_movie_prompt({"year_from":"2026", "year_to":"2020"})

    def test_recommendation_invokes_isolated_sdk_runner(self) -> None:
        response = {"movies":[{
            "title_ru":"Тест", "title_original":"Test", "year":2024, "comment":"Подходит по настроению",
        }]}
        details = {
            "title_ru":"Тест", "title_original":"Test", "year":"2024", "release_date":"2024-01-01",
            "duration_minutes":120, "imdb_rating":7.5, "tmdb_id":999, "status":"backlog", "reaction":"",
        }
        completed = subprocess.CompletedProcess([], 0, json.dumps(response, ensure_ascii=False), "")
        with patch.object(llm, "VENV_PYTHON", Path(sys.executable)), \
             patch.object(llm, "get_model", return_value="test-model"), \
             patch.object(llm.tmdb, "get_api_key", return_value=("test-key", "test")), \
             patch.object(llm.tmdb, "resolve_movie", return_value=999), \
             patch.object(llm.tmdb, "movie_details", return_value=details), \
             patch.object(llm.subprocess, "run", return_value=completed) as run:
            result = llm.recommend_movies({"prompt":"Научная фантастика", "year_to":"2026", "limit":"1"})
        self.assertEqual(result["items"][0]["notes"], "Подходит по настроению")
        self.assertEqual(result["items"][0]["tmdb_id"], 999)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["model"], "test-model")
        self.assertIn("Научная фантастика", run.call_args.kwargs["input"])
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("--model") + 1], "test-model")
        self.assertEqual(command[command.index("--schema") + 1], str(llm.SCHEMA_PATH))

    def test_recommendation_skips_movies_that_fail_verified_filters(self) -> None:
        response = {"movies":[{
            "title_ru":"Короткий фильм", "title_original":"Short Movie", "year":2024, "comment":"Проверка",
        }]}
        completed = subprocess.CompletedProcess([], 0, json.dumps(response), "")
        details = {
            "title_ru":"Короткий фильм", "title_original":"Short Movie", "year":"2024",
            "duration_minutes":80, "imdb_rating":7.5, "tmdb_id":1000,
        }
        with patch.object(llm, "VENV_PYTHON", Path(sys.executable)), \
             patch.object(llm.tmdb, "get_api_key", return_value=("test-key", "test")), \
             patch.object(llm.tmdb, "resolve_movie", return_value=1000), \
             patch.object(llm.tmdb, "movie_details", return_value=details), \
             patch.object(llm.subprocess, "run", return_value=completed):
            result = llm.recommend_movies({"year_to":"2026", "limit":"1", "min_runtime":"100"})
        self.assertEqual(result["items"], [])
        self.assertIn("длительность меньше 100", result["errors"][0]["error"])

    def test_rejects_unstructured_model_response(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "1. Тест (Test)", "")
        with patch.object(llm, "VENV_PYTHON", Path(sys.executable)), \
             patch.object(llm.subprocess, "run", return_value=completed):
            with self.assertRaises(llm.LlmError):
                llm.recommend_movies({"year_to":"2026"})

    def test_accepts_empty_structured_response(self) -> None:
        completed = subprocess.CompletedProcess([], 0, '{"movies":[]}', "")
        with patch.object(llm, "VENV_PYTHON", Path(sys.executable)), \
             patch.object(llm.tmdb, "get_api_key", return_value=("test-key", "test")), \
             patch.object(llm.subprocess, "run", return_value=completed):
            result = llm.recommend_movies({"year_to":"2026", "limit":"3"})
        self.assertEqual(result["items"], [])
        self.assertEqual(result["errors"], [])


if __name__ == "__main__":
    unittest.main()
