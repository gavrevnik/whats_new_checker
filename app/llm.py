from __future__ import annotations

import json
import os
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
    imdb_rating = _number(payload, "min_imdb_rating", 6.5, 0, 10)
    year_from = int(_number(payload, "year_from", 2020, 1888, current_year + 2))
    year_to = int(_number(payload, "year_to", current_year, 1888, current_year + 2))
    min_runtime = int(_number(payload, "min_runtime", 100, 0, 600))
    limit = int(_number(payload, "limit", 10, 1, 20))
    if year_from > year_to:
        raise LlmError("Начальный год не может быть больше конечного")
    user_prompt = str(payload.get("prompt") or "").strip()
    if len(user_prompt) > 8000:
        raise LlmError("Пользовательский промпт слишком длинный")

    movies = storage.list_library(content_type="movie")
    liked = [_movie_line(item) for item in movies if item.get("status") == "consumed" and item.get("reaction") == "like"]
    backlog = [_movie_line(item) for item in movies if item.get("status") == "backlog"]
    people = [_person_line(person) for person in storage.list_interests("movie")]
    user_block = user_prompt or "Дополнительных пожеланий нет — подбери наиболее релевантные фильмы по профилю вкусов."

    return "\n\n".join([
        "Задача: порекомендуй фильмы с русским и оригинальным названием, соблюдая условия ниже. "
        f"Верни {limit} наиболее релевантных позиций (меньше — только если фильтрам действительно не соответствует достаточно фильмов). "
        "Не включай фильмы из бэклога или просмотренные фильмы. Для каждой позиции дай короткий содержательный "
        "комментарий на русском: почему фильм подходит именно этому пользователю.",
        _section("Обязательные фильтры", [
            f"- IMDb rating не ниже {imdb_rating:g}",
            f"- год выпуска от {year_from} до {year_to} включительно",
            f"- длительность не меньше {min_runtime} минут",
        ]),
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
    min_imdb = _number(payload, "min_imdb_rating", 6.5, 0, 10)
    year_from = int(_number(payload, "year_from", 2020, 1888, current_year + 2))
    year_to = int(_number(payload, "year_to", current_year, 1888, current_year + 2))
    min_runtime = int(_number(payload, "min_runtime", 100, 0, 600))
    rating = item.get("imdb_rating")
    year = str(item.get("year") or "")
    runtime = item.get("duration_minutes")
    if rating in (None, "") or float(rating) < min_imdb:
        return f"IMDb rating ниже {min_imdb:g} или недоступен"
    if not year.isdigit() or not year_from <= int(year) <= year_to:
        return f"год выпуска вне диапазона {year_from}–{year_to}"
    if runtime in (None, "") or int(runtime) < min_runtime:
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
    requested = int(_number(payload, "limit", 10, 1, 20))
    items, errors = _enrich_movies(candidates[:requested], payload)
    return {"items": items, "errors": errors, "model": model, "requested": requested, "received": len(candidates)}
