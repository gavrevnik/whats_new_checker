from __future__ import annotations

import json
import os
import random
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

from app import storage, tmdb
from app.storage import ROOT


RUNNER = ROOT / "scripts" / "run_codex_recommendation.py"
SCHEMA_PATH = ROOT / "app" / "movie_recommendations.schema.json"
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
DEFAULT_MODEL = "gpt-5.6-terra"


class LlmError(RuntimeError):
    pass


def get_model() -> str:
    return (
        os.environ.get("CODEX_RECOMMENDATION_MODEL", "").strip()
        or tmdb._local_secrets().get("CODEX_RECOMMENDATION_MODEL", "").strip()
        or DEFAULT_MODEL
    )


def configuration() -> dict[str, Any]:
    return {
        "configured": VENV_PYTHON.exists() and RUNNER.exists() and SCHEMA_PATH.exists(),
        "provider": "Codex SDK",
        "model": get_model(),
        "auth": "ChatGPT/Codex local login",
    }


def _number(payload: dict[str, Any], key: str, default: float, minimum: float, maximum: float) -> float:
    raw = payload.get(key)
    try:
        value = default if raw in (None, "") else float(raw)
    except (TypeError, ValueError) as error:
        raise LlmError(f"Invalid value for {key}") from error
    if not minimum <= value <= maximum:
        raise LlmError(f"{key} must be between {minimum:g} and {maximum:g}")
    return value


def _optional_number(
    payload: dict[str, Any], key: str, default: float, minimum: float, maximum: float,
) -> float | None:
    disabled = payload.get("disabled_filters", [])
    if not isinstance(disabled, list):
        raise LlmError("disabled_filters must be an array")
    if key in {str(value) for value in disabled}:
        return None
    return _number(payload, key, default, minimum, maximum)


def _movie_line(item: dict[str, Any]) -> str:
    names = str(item.get("title_ru") or item.get("title_original") or "Без названия")
    original = str(item.get("title_original") or "")
    if original and original.casefold() != names.casefold():
        names += f" ({original})"
    details = [str(item.get("year") or ""), str(item.get("directors") or "")]
    suffix = ", ".join(value for value in details if value)
    return f"- {names}{f' — {suffix}' if suffix else ''}"


def _person_line(person: dict[str, Any]) -> str:
    localized = str(person.get("name_ru") or person.get("name_original") or "")
    original = str(person.get("name_original") or "")
    name = localized if not original or localized.casefold() == original.casefold() else f"{localized} ({original})"
    role = "режиссёр" if person.get("role") == "director" else "актёр"
    return f"- {name} — {role}"


def _section(title: str, lines: list[str]) -> str:
    return f"{title}:\n" + ("\n".join(lines) if lines else "- нет данных")


def build_movie_prompt(payload: dict[str, Any]) -> str:
    current_year = date.today().year
    imdb_rating = _optional_number(payload, "min_imdb_rating", 7, 0, 10)
    kinopoisk_rating = _optional_number(payload, "min_kinopoisk_rating", 0, 0, 10)
    year_from_raw = _optional_number(payload, "year_from", 2020, 1888, current_year + 2)
    year_to_raw = _optional_number(payload, "year_to", current_year, 1888, current_year + 2)
    runtime_raw = _optional_number(payload, "min_runtime", 100, 0, 600)
    year_from = int(year_from_raw) if year_from_raw is not None else None
    year_to = int(year_to_raw) if year_to_raw is not None else None
    min_runtime = int(runtime_raw) if runtime_raw is not None else None
    liked_sample_size = int(_number(payload, "liked_sample_size", 30, 0, 200))
    limit_raw = _optional_number(payload, "limit", 5, 1, 20)
    limit = int(limit_raw) if limit_raw is not None else None
    if year_from is not None and year_to is not None and year_from > year_to:
        raise LlmError("Начальный год не может быть больше конечного")
    user_prompt = str(payload.get("prompt") or "").strip()
    if len(user_prompt) > 8000:
        raise LlmError("Пользовательский промпт слишком длинный")

    movies = storage.list_library(content_type="movie")
    liked_movies = [item for item in movies if item.get("status") == "consumed" and item.get("reaction") == "like"]
    liked = [
        _movie_line(item)
        for item in random.sample(liked_movies, min(liked_sample_size, len(liked_movies)))
    ]
    backlog = [_movie_line(item) for item in movies if item.get("status") == "backlog"]
    people = [_person_line(person) for person in storage.list_interests("movie")]
    user_block = user_prompt or "Дополнительных пожеланий нет — подбери наиболее релевантные фильмы по профилю вкусов."

    filters = []
    if imdb_rating is not None:
        filters.append(
            f"- у фильма обязательно должен быть опубликованный IMDb rating не ниже {imdb_rating:g}; "
            "фильмы без IMDb rating не предлагай"
        )
    if kinopoisk_rating is not None and kinopoisk_rating > 0:
        filters.append(
            f"- у фильма обязательно должен быть опубликованный рейтинг Кинопоиска не ниже {kinopoisk_rating:g}; "
            "фильмы без рейтинга Кинопоиска не предлагай"
        )
    if year_from is not None and year_to is not None:
        filters.append(f"- год выпуска от {year_from} до {year_to} включительно")
    elif year_from is not None:
        filters.append(f"- год выпуска не раньше {year_from}")
    elif year_to is not None:
        filters.append(f"- год выпуска не позже {year_to}")
    if min_runtime is not None:
        filters.append(f"- длительность не меньше {min_runtime} минут")
    if limit is not None:
        filters.append(f"- верни не более {limit} фильмов, соответствующих всем включённым условиям")

    return "\n\n".join([
        "Задача: порекомендуй фильмы с русским и оригинальным названием, соблюдая условия ниже. "
        f"Верни не более {limit if limit is not None else 10} наиболее релевантных позиций. "
        "Не включай фильмы из бэклога или просмотренные фильмы. Для каждой позиции дай короткий содержательный "
        "комментарий на русском: почему фильм подходит именно этому пользователю. Все включённые условия являются жёсткими: "
        "проверь их до формирования ответа и не предлагай фильм, если не можешь уверенно подтвердить соответствующее значение.",
        _section("Обязательные фильтры", filters),
        f"Свободный запрос пользователя:\n{user_block}",
        _section("Фильмы, которые понравились пользователю", liked),
        _section(
            "Любимые актёры и режиссёры (используй как мягкий сигнал вкуса; их участие не обязательно, если пользователь прямо этого не потребовал)",
            people,
        ),
        _section("Текущий бэклог — эти фильмы нельзя повторять", backlog),
        "Формат ответа задаётся JSON Schema. Верни только JSON-объект с массивом movies. У каждого фильма обязательны "
        "поля title_ru (официальное или наиболее употребимое русское название), title_original (оригинальное название), "
        "year (целое число) и comment (комментарий на русском). Не добавляй вступление, Markdown, вопросы пользователю "
        "или фильмы, нарушающие обязательные фильтры.",
    ])


def _parse_response(raw: str) -> list[dict[str, Any]]:
    try:
        response = json.loads(raw)
    except json.JSONDecodeError as error:
        raise LlmError(f"Codex вернул ответ, который не удалось разобрать как JSON: {error}") from error
    movies = response.get("movies") if isinstance(response, dict) else None
    if not isinstance(movies, list):
        raise LlmError("Codex вернул JSON без массива movies")
    parsed: list[dict[str, Any]] = []
    for index, movie in enumerate(movies, start=1):
        if not isinstance(movie, dict):
            raise LlmError(f"Позиция {index} в ответе Codex имеет неверный формат")
        title_ru = str(movie.get("title_ru") or "").strip()
        title_original = str(movie.get("title_original") or "").strip()
        comment = str(movie.get("comment") or "").strip()
        try:
            year = int(movie.get("year"))
        except (TypeError, ValueError) as error:
            raise LlmError(f"У позиции {index} отсутствует корректный год") from error
        if not title_ru or not title_original or not comment or not 1888 <= year <= 2100:
            raise LlmError(f"У позиции {index} не заполнены обязательные поля")
        parsed.append({"title_ru": title_ru, "title_original": title_original, "year": year, "comment": comment})
    return parsed


def _known_title(candidate: dict[str, Any], known_titles: set[str]) -> bool:
    year = str(candidate.get("year") or "")
    for field in ("title_original", "title_ru"):
        normalized = storage._normalized(str(candidate.get(field) or ""))
        if normalized and (normalized in known_titles or f"{normalized}:{year}" in known_titles):
            return True
    return False


def _enrich_candidate(candidate: dict[str, Any], api_key: str) -> dict[str, Any]:
    tmdb_id = tmdb.resolve_movie(candidate["title_original"], candidate["title_ru"], candidate["year"], api_key)
    details = tmdb.movie_details(tmdb_id, api_key)
    details.update({
        "notes": candidate["comment"],
        "llm_comment": candidate["comment"],
        "raw_data": {
            "title_ru": candidate["title_ru"],
            "title_original": candidate["title_original"],
            "year": candidate["year"],
            "notes": candidate["comment"],
        },
    })
    return details


def _passes_filters(item: dict[str, Any], payload: dict[str, Any]) -> str:
    current_year = date.today().year
    min_imdb = _optional_number(payload, "min_imdb_rating", 7, 0, 10)
    min_kinopoisk = _optional_number(payload, "min_kinopoisk_rating", 0, 0, 10)
    year_from_raw = _optional_number(payload, "year_from", 2020, 1888, current_year + 2)
    year_to_raw = _optional_number(payload, "year_to", current_year, 1888, current_year + 2)
    runtime_raw = _optional_number(payload, "min_runtime", 100, 0, 600)
    year_from = int(year_from_raw) if year_from_raw is not None else None
    year_to = int(year_to_raw) if year_to_raw is not None else None
    min_runtime = int(runtime_raw) if runtime_raw is not None else None
    rating = item.get("imdb_rating")
    kinopoisk_rating = item.get("kinopoisk_rating")
    year = str(item.get("year") or "")
    runtime = item.get("duration_minutes")
    if min_imdb is not None and (rating in (None, "") or float(rating) < min_imdb):
        return f"IMDb rating ниже {min_imdb:g} или недоступен"
    if min_kinopoisk is not None and min_kinopoisk > 0:
        # Kinopoisk constrains the model prompt, but a missing companion rating
        # must not hide an otherwise valid TMDB recommendation card.
        if kinopoisk_rating not in (None, "") and float(kinopoisk_rating) < min_kinopoisk:
            return f"рейтинг Кинопоиска ниже {min_kinopoisk:g}"
    if year_from is not None and (not year.isdigit() or int(year) < year_from):
        return f"год выпуска раньше {year_from}"
    if year_to is not None and (not year.isdigit() or int(year) > year_to):
        return f"год выпуска позже {year_to}"
    if min_runtime is not None and (runtime in (None, "") or int(runtime) < min_runtime):
        return f"длительность меньше {min_runtime} мин. или недоступна"
    return ""


def _enrich_movies(candidates: list[dict[str, Any]], payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    api_key, _ = tmdb.get_api_key()
    if not api_key:
        raise LlmError("TMDB_API_KEY не настроен: невозможно уточнить рекомендации")
    known_ids, known_titles = storage.known_movie_keys()
    errors: list[dict[str, str]] = []
    results: list[tuple[int, dict[str, Any]]] = []

    eligible: list[tuple[int, dict[str, Any]]] = []
    for index, candidate in enumerate(candidates):
        label = f"{candidate['title_ru']} ({candidate['title_original']}, {candidate['year']})"
        if _known_title(candidate, known_titles):
            errors.append({"title": label, "error": "уже есть в библиотеке или корзине"})
        else:
            eligible.append((index, candidate))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_enrich_candidate, candidate, api_key): (index, candidate) for index, candidate in eligible}
        for future in as_completed(futures):
            index, candidate = futures[future]
            label = f"{candidate['title_ru']} ({candidate['title_original']}, {candidate['year']})"
            try:
                item = future.result()
                reason = _passes_filters(item, payload)
                if reason:
                    errors.append({"title": label, "error": reason})
                    continue
                if str(item.get("tmdb_id") or "") in known_ids or _known_title(item, known_titles):
                    errors.append({"title": label, "error": "уже есть в библиотеке или корзине"})
                    continue
                results.append((index, item))
            except Exception as error:
                errors.append({"title": label, "error": str(error)})

    items: list[dict[str, Any]] = []
    seen_tmdb_ids: set[str] = set()
    for _, item in sorted(results, key=lambda row: row[0]):
        tmdb_id = str(item.get("tmdb_id") or "")
        if tmdb_id in seen_tmdb_ids:
            continue
        seen_tmdb_ids.add(tmdb_id)
        items.append(item)
    return items, errors


def recommend_movies(payload: dict[str, Any]) -> dict[str, Any]:
    prompt = build_movie_prompt(payload)
    if not VENV_PYTHON.exists():
        raise LlmError("Codex SDK не установлен. Выполните: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt")
    model = get_model()
    try:
        completed = subprocess.run(
            [str(VENV_PYTHON), str(RUNNER), "--model", model, "--schema", str(SCHEMA_PATH)],
            input=prompt,
            text=True,
            capture_output=True,
            cwd=ROOT,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise LlmError("Codex не успел ответить за 5 минут") from error
    except OSError as error:
        raise LlmError(f"Не удалось запустить Codex SDK: {error}") from error
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or "unknown Codex SDK error"
        raise LlmError(f"Codex SDK: {message[-1200:]}")
    answer = completed.stdout.strip()
    if not answer:
        raise LlmError("Codex вернул пустой ответ")
    candidates = _parse_response(answer)
    limit_raw = _optional_number(payload, "limit", 5, 1, 20)
    requested = int(limit_raw) if limit_raw is not None else 10
    items, errors = _enrich_movies(candidates[:requested], payload)
    return {"items": items, "errors": errors, "model": model, "requested": requested, "received": len(candidates)}
