from __future__ import annotations

import json
import re
import sqlite3
import threading
import unicodedata
import uuid
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "library.sqlite3"

VALID_STATUSES = {"backlog", "consumed", "dismissed"}
VALID_REACTIONS = {"", "like", "dislike"}
_lock = threading.RLock()

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_types (
    code TEXT PRIMARY KEY,
    name_ru TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1))
);

CREATE TABLE IF NOT EXISTS content_items (
    id TEXT PRIMARY KEY,
    content_type TEXT NOT NULL REFERENCES content_types(code),
    title_ru TEXT NOT NULL,
    title_original TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'backlog' CHECK (status IN ('backlog', 'consumed', 'dismissed')),
    reaction TEXT NOT NULL DEFAULT '' CHECK (reaction IN ('', 'like', 'dislike')),
    source TEXT NOT NULL DEFAULT 'manual',
    source_url TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    added_at TEXT NOT NULL,
    consumed_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movies (
    content_id TEXT PRIMARY KEY REFERENCES content_items(id) ON DELETE CASCADE,
    release_date TEXT,
    release_year INTEGER,
    runtime_minutes INTEGER,
    imdb_rating REAL,
    kinopoisk_rating REAL,
    tmdb_rating REAL,
    tmdb_vote_count INTEGER,
    imdb_id TEXT,
    kinopoisk_id INTEGER,
    tmdb_id INTEGER UNIQUE,
    overview TEXT NOT NULL DEFAULT '',
    original_language TEXT NOT NULL DEFAULT '',
    awards_json TEXT NOT NULL DEFAULT '[]',
    tagline TEXT NOT NULL DEFAULT '',
    content_rating TEXT NOT NULL DEFAULT '',
    imdb_votes TEXT NOT NULL DEFAULT '',
    metascore INTEGER,
    box_office TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    tmdb_updated_at TEXT
);

CREATE TABLE IF NOT EXISTS people (
    id TEXT PRIMARY KEY,
    name_original TEXT NOT NULL,
    name_ru TEXT NOT NULL,
    tmdb_id INTEGER UNIQUE,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    raw_json TEXT NOT NULL DEFAULT '{}',
    details_json TEXT NOT NULL DEFAULT '{}',
    tmdb_updated_at TEXT
);

CREATE TABLE IF NOT EXISTS interest_roles (
    person_id TEXT NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    content_type TEXT NOT NULL REFERENCES content_types(code),
    role TEXT NOT NULL CHECK (role IN ('actor', 'director')),
    notes TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (person_id, content_type, role)
);

CREATE TABLE IF NOT EXISTS movie_people (
    movie_id TEXT NOT NULL REFERENCES movies(content_id) ON DELETE CASCADE,
    person_id TEXT NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    credit_role TEXT NOT NULL CHECK (credit_role IN ('actor', 'director')),
    character_name TEXT NOT NULL DEFAULT '',
    job TEXT NOT NULL DEFAULT '',
    is_interest INTEGER NOT NULL DEFAULT 0 CHECK (is_interest IN (0, 1)),
    PRIMARY KEY (movie_id, person_id, credit_role)
);

CREATE TABLE IF NOT EXISTS genres (
    id INTEGER PRIMARY KEY,
    name_ru TEXT NOT NULL,
    name_original TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id TEXT NOT NULL REFERENCES movies(content_id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES genres(id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS content_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id TEXT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL DEFAULT '',
    external_id TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    UNIQUE (content_id, alias, provider, external_id)
);

CREATE TABLE IF NOT EXISTS trash_entries (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('movie', 'person')),
    entity_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT '',
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    trashed_at TEXT NOT NULL,
    UNIQUE (entity_type, entity_id, role)
);

CREATE INDEX IF NOT EXISTS idx_content_state ON content_items(content_type, status, reaction);
CREATE INDEX IF NOT EXISTS idx_movie_people_interest ON movie_people(movie_id, is_interest);
CREATE INDEX IF NOT EXISTS idx_alias_value ON content_aliases(alias);
CREATE INDEX IF NOT EXISTS idx_trash_entity ON trash_entries(entity_type, entity_id, role);
"""


class StorageError(ValueError):
    pass


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalized(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "").casefold()
    return re.sub(r"[^\w]+", "", value)


def _json(value: Any, default: str) -> str:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def connect(path: Path | None = None) -> sqlite3.Connection:
    target = path or DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target, timeout=20, factory=ClosingConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 20000")
    return connection


def initialize_database(path: Path | None = None) -> None:
    with _lock, connect(path) as connection:
        connection.executescript(SCHEMA)
        migrations = {
            "content_items": {
                "raw_json": "TEXT NOT NULL DEFAULT '{}'",
            },
            "movies": {
                "kinopoisk_rating": "REAL",
                "kinopoisk_id": "INTEGER",
                "tagline": "TEXT NOT NULL DEFAULT ''",
                "content_rating": "TEXT NOT NULL DEFAULT ''",
                "imdb_votes": "TEXT NOT NULL DEFAULT ''",
                "metascore": "INTEGER",
                "box_office": "TEXT NOT NULL DEFAULT ''",
                "details_json": "TEXT NOT NULL DEFAULT '{}'",
            },
            "people": {
                "raw_json": "TEXT NOT NULL DEFAULT '{}'",
                "details_json": "TEXT NOT NULL DEFAULT '{}'",
                "tmdb_updated_at": "TEXT",
            },
        }
        for table, columns in migrations.items():
            existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            for column, definition in columns.items():
                if column not in existing:
                    connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        connection.execute(
            "INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (1, ?)", (_now(),)
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (2, ?)", (_now(),)
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (3, ?)", (_now(),)
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (4, ?)", (_now(),)
        )
        connection.execute(
            "INSERT OR IGNORE INTO content_types(code, name_ru, enabled) VALUES ('movie', 'Фильмы', 1)"
        )


def _movie_select(where: str = "", params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    initialize_database()
    query = f"""
        SELECT
            i.id, i.content_type, i.title_ru, i.title_original, i.status, i.reaction,
            i.source, i.source_url AS url, i.notes, COALESCE(i.raw_json, '{{}}') AS raw_json,
            i.metadata_json, i.added_at,
            COALESCE(i.consumed_at, '') AS consumed_at, i.updated_at,
            COALESCE(m.release_date, '') AS release_date,
            COALESCE(m.release_year, '') AS year,
            COALESCE(m.runtime_minutes, '') AS duration_minutes,
            COALESCE(m.imdb_rating, '') AS imdb_rating,
            COALESCE(m.kinopoisk_rating, '') AS kinopoisk_rating,
            COALESCE(m.tmdb_rating, '') AS tmdb_rating,
            COALESCE(m.tmdb_vote_count, '') AS tmdb_vote_count,
            COALESCE(m.imdb_id, '') AS imdb_id,
            COALESCE(m.kinopoisk_id, '') AS kinopoisk_id,
            COALESCE(m.tmdb_id, '') AS tmdb_id,
            COALESCE(m.overview, '') AS overview,
            COALESCE(m.original_language, '') AS original_language,
            COALESCE(m.awards_json, '[]') AS awards_json,
            COALESCE(m.tagline, '') AS tagline,
            COALESCE(m.content_rating, '') AS content_rating,
            COALESCE(m.imdb_votes, '') AS imdb_votes,
            COALESCE(m.metascore, '') AS metascore,
            COALESCE(m.box_office, '') AS box_office,
            COALESCE(m.details_json, '{{}}') AS details_json,
            COALESCE(m.tmdb_updated_at, '') AS tmdb_updated_at,
            COALESCE((
                SELECT group_concat(display_name, '; ')
                FROM (
                    SELECT CASE
                        WHEN p.name_ru = '' THEN p.name_original
                        WHEN p.name_original = '' OR p.name_ru = p.name_original THEN p.name_ru
                        ELSE p.name_ru || ' (' || p.name_original || ')'
                    END AS display_name
                    FROM movie_people mp JOIN people p ON p.id = mp.person_id
                    WHERE mp.movie_id = i.id AND mp.credit_role = 'director'
                    ORDER BY p.name_ru, p.name_original
                )
            ), '') AS directors,
            COALESCE((
                SELECT group_concat(name_ru, '; ')
                FROM (
                    SELECT g.name_ru
                    FROM movie_genres mg JOIN genres g ON g.id = mg.genre_id
                    WHERE mg.movie_id = i.id
                    ORDER BY g.name_ru
                )
            ), '') AS genres,
            COALESCE((
                SELECT group_concat(display_name, '; ')
                FROM (
                    SELECT CASE
                        WHEN p.name_ru = '' THEN p.name_original
                        WHEN p.name_original = '' OR p.name_ru = p.name_original THEN p.name_ru
                        ELSE p.name_ru || ' (' || p.name_original || ')'
                    END AS display_name
                    FROM movie_people mp JOIN people p ON p.id = mp.person_id
                    WHERE mp.movie_id = i.id AND mp.is_interest = 1 AND mp.credit_role = 'actor'
                    ORDER BY p.name_ru, p.name_original
                )
            ), '') AS key_actors,
            COALESCE((
                SELECT group_concat(display_name, '; ')
                FROM (
                    SELECT CASE
                        WHEN p.name_ru = '' THEN p.name_original
                        WHEN p.name_original = '' OR p.name_ru = p.name_original THEN p.name_ru
                        ELSE p.name_ru || ' (' || p.name_original || ')'
                    END AS display_name
                    FROM movie_people mp JOIN people p ON p.id = mp.person_id
                    WHERE mp.movie_id = i.id AND mp.is_interest = 1 AND mp.credit_role = 'director'
                    ORDER BY p.name_ru, p.name_original
                )
            ), '') AS key_directors
        FROM content_items i
        JOIN movies m ON m.content_id = i.id
        {where}
        ORDER BY COALESCE(m.release_date, printf('%04d', m.release_year), '') DESC, i.title_ru
    """
    with connect() as connection:
        results = [dict(row) for row in connection.execute(query, params).fetchall()]
    for result in results:
        try:
            details = json.loads(result.get("details_json") or "{}")
        except json.JSONDecodeError:
            details = {}
        if isinstance(details, dict):
            result.update(details)
        imdb_id = str(result.get("imdb_id") or "")
        tmdb_id = str(result.get("tmdb_id") or "")
        kinopoisk_id = str(result.get("kinopoisk_id") or "")
        result["imdb_link"] = f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else ""
        result["tmdb_link"] = f"https://www.themoviedb.org/movie/{tmdb_id}" if tmdb_id else ""
        result["kinopoisk_link"] = f"https://www.kinopoisk.ru/film/{kinopoisk_id}/" if kinopoisk_id else ""
        result["external_link"] = result["imdb_link"] or result["tmdb_link"] or str(result.get("url") or "")
        key_parts = []
        if result.get("key_actors"):
            key_parts.append(f"Актёры: {result['key_actors']}")
        if result.get("key_directors"):
            key_parts.append(f"Режиссёры: {result['key_directors']}")
        result["key_people"] = "; ".join(key_parts)
    return results


def list_library(
    content_type: str | None = None,
    status: str | None = None,
    include_trashed: bool = False,
) -> list[dict[str, Any]]:
    clauses = ["i.content_type = 'movie'"]
    params: list[Any] = []
    if content_type:
        clauses.append("i.content_type = ?")
        params.append(content_type)
    if status:
        clauses.append("i.status = ?")
        params.append(status)
    if not include_trashed:
        clauses.append(
            "NOT EXISTS (SELECT 1 FROM trash_entries t "
            "WHERE t.entity_type = 'movie' AND t.entity_id = i.id)"
        )
    return _movie_select("WHERE " + " AND ".join(clauses), tuple(params))


def get_item(item_id: str) -> dict[str, Any]:
    rows = _movie_select("WHERE i.id = ?", (item_id,))
    if not rows:
        raise StorageError("Item not found")
    return rows[0]


def list_interests(content_type: str | None = None, role: str | None = None) -> list[dict[str, Any]]:
    initialize_database()
    clauses = [
        "p.active = 1",
        "NOT EXISTS (SELECT 1 FROM trash_entries t WHERE t.entity_type = 'person' "
        "AND t.entity_id = p.id AND t.role = ir.role)",
    ]
    params: list[Any] = []
    if content_type:
        clauses.append("ir.content_type = ?")
        params.append(content_type)
    if role:
        clauses.append("ir.role = ?")
        params.append(role)
    query = f"""
        SELECT p.id, ir.content_type, ir.role, p.name_original, p.name_ru,
               'tmdb' AS provider, COALESCE(p.tmdb_id, '') AS external_id,
               COALESCE(p.tmdb_id, '') AS tmdb_id, p.active, ir.notes,
               COALESCE(p.raw_json, '{{}}') AS raw_json,
               COALESCE(p.details_json, '{{}}') AS details_json,
               COALESCE(p.tmdb_updated_at, '') AS tmdb_updated_at
        FROM people p JOIN interest_roles ir ON ir.person_id = p.id
        WHERE {' AND '.join(clauses)}
        ORDER BY ir.role, p.name_original
    """
    with connect() as connection:
        return [dict(row) for row in connection.execute(query, tuple(params)).fetchall()]


def list_trash() -> list[dict[str, Any]]:
    initialize_database()
    with connect() as connection:
        rows = connection.execute(
            "SELECT id, entity_type, entity_id, role, snapshot_json, trashed_at "
            "FROM trash_entries ORDER BY trashed_at DESC"
        ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        result = dict(row)
        try:
            snapshot = json.loads(result.pop("snapshot_json") or "{}")
        except json.JSONDecodeError:
            snapshot = {}
        if isinstance(snapshot, dict):
            result.update(snapshot)
        results.append(result)
    return results


def trash_entity(payload: dict[str, Any]) -> dict[str, Any]:
    entity_type = str(payload.get("entity_type") or "").strip()
    entity_id = str(payload.get("entity_id") or payload.get("id") or "").strip()
    role = str(payload.get("role") or "").strip() if entity_type == "person" else ""
    if entity_type not in {"movie", "person"} or not entity_id:
        raise StorageError("Movie or person is required")
    if entity_type == "person" and role not in {"actor", "director"}:
        raise StorageError("Person role must be actor or director")
    initialize_database()
    trash_id = f"trash-{uuid.uuid4().hex[:12]}"
    with _lock, connect() as connection:
        if entity_type == "movie":
            row = connection.execute(
                "SELECT i.title_ru, i.title_original, COALESCE(m.release_year, '') AS year "
                "FROM content_items i JOIN movies m ON m.content_id = i.id WHERE i.id = ?",
                (entity_id,),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT p.name_ru, p.name_original, COALESCE(p.tmdb_id, '') AS tmdb_id "
                "FROM people p JOIN interest_roles ir ON ir.person_id = p.id "
                "WHERE p.id = ? AND ir.content_type = 'movie' AND ir.role = ?",
                (entity_id, role),
            ).fetchone()
        if not row:
            raise StorageError("Movie or person not found")
        try:
            connection.execute(
                "INSERT INTO trash_entries(id, entity_type, entity_id, role, snapshot_json, trashed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (trash_id, entity_type, entity_id, role, _json(dict(row), "{}"), _now()),
            )
        except sqlite3.IntegrityError as error:
            raise StorageError("This item is already in trash") from error
    return next(item for item in list_trash() if item["id"] == trash_id)


def restore_trash(trash_id: str) -> dict[str, Any]:
    initialize_database()
    with _lock, connect() as connection:
        row = connection.execute(
            "SELECT id, entity_type, entity_id, role, snapshot_json, trashed_at "
            "FROM trash_entries WHERE id = ?",
            (trash_id,),
        ).fetchone()
        if not row:
            raise StorageError("Trash item not found")
        result = dict(row)
        connection.execute("DELETE FROM trash_entries WHERE id = ?", (trash_id,))
    try:
        snapshot = json.loads(result.pop("snapshot_json") or "{}")
    except json.JSONDecodeError:
        snapshot = {}
    if isinstance(snapshot, dict):
        result.update(snapshot)
    return result


def add_interest_person(person: dict[str, Any]) -> dict[str, Any]:
    role = str(person.get("role") or "").strip()
    if role not in {"actor", "director"}:
        raise StorageError("Person role must be actor or director")
    name_original = str(person.get("name_original") or person.get("name_ru") or "").strip()
    name_ru = str(person.get("name_ru") or name_original).strip()
    if not name_original or not name_ru:
        raise StorageError("Person name is required")
    tmdb_raw = person.get("tmdb_id") or person.get("external_id")
    tmdb_id = int(tmdb_raw) if str(tmdb_raw).isdigit() else None
    raw_json = _json(
        person.get("raw_data") or {
            key: person.get(key) for key in ("role", "tmdb_id", "name_original", "name_ru")
            if person.get(key) not in (None, "")
        },
        "{}",
    )
    initialize_database()
    with _lock, connect() as connection:
        existing = None
        if tmdb_id:
            existing = connection.execute("SELECT id FROM people WHERE tmdb_id = ?", (tmdb_id,)).fetchone()
        if not existing:
            existing = connection.execute(
                "SELECT id FROM people WHERE name_original = ? COLLATE NOCASE OR name_ru = ? COLLATE NOCASE",
                (name_original, name_ru),
            ).fetchone()
        person_id = str(existing["id"]) if existing else f"person-{uuid.uuid4().hex[:12]}"
        if existing:
            connection.execute(
                "UPDATE people SET active = 1, tmdb_id = COALESCE(tmdb_id, ?), "
                "name_original = COALESCE(NULLIF(name_original, ''), ?), name_ru = COALESCE(NULLIF(name_ru, ''), ?), "
                "raw_json = ? WHERE id = ?",
                (tmdb_id, name_original, name_ru, raw_json, person_id),
            )
        else:
            connection.execute(
                "INSERT INTO people(id, name_original, name_ru, tmdb_id, active, raw_json) VALUES (?, ?, ?, ?, 1, ?)",
                (person_id, name_original, name_ru, tmdb_id, raw_json),
            )
        if connection.execute(
            "SELECT 1 FROM interest_roles WHERE person_id = ? AND content_type = 'movie' AND role = ?",
            (person_id, role),
        ).fetchone():
            raise StorageError("This person is already in this list")
        connection.execute(
            "INSERT INTO interest_roles(person_id, content_type, role, notes) VALUES (?, 'movie', ?, ?)",
            (person_id, role, str(person.get("notes") or "")),
        )
    return next(row for row in list_interests("movie", role) if row["id"] == person_id)


def update_interest_person(person_id: str, details: dict[str, Any]) -> dict[str, Any]:
    initialize_database()
    tmdb_raw = details.get("tmdb_id") or details.get("external_id")
    tmdb_id = int(tmdb_raw) if str(tmdb_raw).isdigit() else None
    with _lock, connect() as connection:
        row = connection.execute("SELECT id FROM people WHERE id = ?", (person_id,)).fetchone()
        if not row:
            raise StorageError("Person not found")
        duplicate = connection.execute(
            "SELECT id FROM people WHERE tmdb_id = ? AND id <> ?", (tmdb_id, person_id),
        ).fetchone() if tmdb_id else None
        if duplicate:
            duplicate_id = str(duplicate["id"])
            duplicate_roles = connection.execute(
                "SELECT 1 FROM interest_roles WHERE person_id = ?", (duplicate_id,),
            ).fetchone()
            if duplicate_roles:
                raise StorageError("Another interest person already uses this TMDB ID")
            credits = connection.execute(
                "SELECT movie_id, credit_role, character_name, job FROM movie_people WHERE person_id = ?",
                (duplicate_id,),
            ).fetchall()
            for credit in credits:
                connection.execute(
                    "INSERT OR REPLACE INTO movie_people(movie_id, person_id, credit_role, character_name, job, is_interest) "
                    "VALUES (?, ?, ?, ?, ?, 1)",
                    (credit["movie_id"], person_id, credit["credit_role"], credit["character_name"], credit["job"]),
                )
            connection.execute("DELETE FROM people WHERE id = ?", (duplicate_id,))
        connection.execute(
            "UPDATE people SET name_original = COALESCE(NULLIF(?, ''), name_original), "
            "name_ru = COALESCE(NULLIF(?, ''), name_ru), tmdb_id = COALESCE(?, tmdb_id), "
            "details_json = ?, tmdb_updated_at = ? WHERE id = ?",
            (
                str(details.get("name_original") or ""), str(details.get("name_ru") or ""), tmdb_id,
                _json(details.get("details_json") or {}, "{}"), _now(), person_id,
            ),
        )
    return next(row for row in list_interests("movie") if row["id"] == person_id)


def list_content_types() -> list[dict[str, Any]]:
    initialize_database()
    with connect() as connection:
        return [dict(row) for row in connection.execute(
            "SELECT code, name_ru FROM content_types WHERE enabled = 1 ORDER BY code"
        )]


def _validate(item: dict[str, Any], partial: bool = False) -> None:
    if not partial and item.get("content_type", "movie") != "movie":
        raise StorageError("Only movie content is enabled")
    if "status" in item and str(item.get("status") or "backlog") not in VALID_STATUSES:
        raise StorageError("Unsupported status")
    if "reaction" in item and str(item.get("reaction") or "") not in VALID_REACTIONS:
        raise StorageError("Unsupported reaction")
    if not partial and not (str(item.get("title_original", "")).strip() and str(item.get("title_ru", "")).strip()):
        raise StorageError("Both original and Russian titles are required")


def _year(item: dict[str, Any]) -> int | None:
    raw = item.get("year") or str(item.get("release_date") or "")[:4]
    return int(raw) if str(raw).isdigit() else None


DETAIL_FIELDS = (
    "cast", "writers", "countries", "production_companies", "spoken_languages", "keywords",
    "budget", "revenue", "homepage", "poster_url", "movie_status",
)


def _detail_payload(item: dict[str, Any]) -> dict[str, Any]:
    return {key: item.get(key) for key in DETAIL_FIELDS if item.get(key) not in (None, "", [])}


def _duplicate(connection: sqlite3.Connection, item: dict[str, Any]) -> bool:
    tmdb_id = item.get("tmdb_id") or item.get("external_id")
    if str(tmdb_id).isdigit() and connection.execute("SELECT 1 FROM movies WHERE tmdb_id = ?", (int(tmdb_id),)).fetchone():
        return True
    candidate_titles = {_normalized(str(item.get("title_original", ""))), _normalized(str(item.get("title_ru", "")))} - {""}
    year = _year(item)
    rows = connection.execute(
        "SELECT i.title_original, i.title_ru, m.release_year FROM content_items i JOIN movies m ON m.content_id = i.id"
    ).fetchall()
    for row in rows:
        existing_titles = {_normalized(row["title_original"]), _normalized(row["title_ru"])} - {""}
        if not candidate_titles & existing_titles:
            continue
        existing_year = row["release_year"]
        if year is None or existing_year is None or abs(year - existing_year) <= 1:
            return True
    return False


def _person_id(
    connection: sqlite3.Connection,
    name_original: str,
    tmdb_id: int | None = None,
    name_ru: str = "",
) -> str:
    row = None
    if tmdb_id:
        row = connection.execute("SELECT id FROM people WHERE tmdb_id = ?", (tmdb_id,)).fetchone()
    if not row:
        row = connection.execute("SELECT id FROM people WHERE name_original = ? COLLATE NOCASE", (name_original,)).fetchone()
    if row:
        person_id = str(row["id"])
        connection.execute(
            "UPDATE people SET name_original = COALESCE(NULLIF(?, ''), name_original), "
            "name_ru = COALESCE(NULLIF(?, ''), name_ru), tmdb_id = COALESCE(?, tmdb_id) WHERE id = ?",
            (name_original, name_ru, tmdb_id, person_id),
        )
        return person_id
    person_id = f"person-{uuid.uuid4().hex[:12]}"
    connection.execute(
        "INSERT INTO people(id, name_original, name_ru, tmdb_id, active) VALUES (?, ?, ?, ?, 0)",
        (person_id, name_original, name_ru or name_original, tmdb_id),
    )
    return person_id


def _replace_movie_relations(connection: sqlite3.Connection, item_id: str, item: dict[str, Any]) -> None:
    if "genres_data" in item or "genres" in item:
        connection.execute("DELETE FROM movie_genres WHERE movie_id = ?", (item_id,))
        genres_data = item.get("genres_data")
        if not isinstance(genres_data, list):
            genres_data = [
                {"id": -(index + 1), "name": name.strip()}
                for index, name in enumerate(str(item.get("genres") or "").split(";")) if name.strip()
            ]
        for genre in genres_data:
            name = str(genre.get("name") or "").strip()
            if not name:
                continue
            genre_id = int(genre.get("id") or -(zlib.crc32(name.encode("utf-8")) + 1))
            connection.execute(
                "INSERT INTO genres(id, name_ru, name_original) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET name_ru = excluded.name_ru",
                (genre_id, name, str(genre.get("original_name") or "")),
            )
            connection.execute("INSERT OR IGNORE INTO movie_genres(movie_id, genre_id) VALUES (?, ?)", (item_id, genre_id))

    if any(key in item for key in ("directors_data", "key_people_data", "directors", "participants")):
        connection.execute("DELETE FROM movie_people WHERE movie_id = ?", (item_id,))
        directors = item.get("directors_data")
        if not isinstance(directors, list):
            directors = [{"name": name.strip()} for name in str(item.get("directors") or item.get("creators") or "").split(";") if name.strip()]
        key_people = item.get("key_people_data")
        if not isinstance(key_people, list):
            key_people = [{"name": name.strip(), "role": "actor"} for name in str(item.get("participants") or "").split(";") if name.strip()]
        people_with_roles = [(person, "director") for person in directors]
        people_with_roles.extend((person, str(person.get("role") or "actor")) for person in key_people)
        for person, default_role in people_with_roles:
            name_original = str(person.get("name_original") or person.get("name") or "").strip()
            name_ru = str(person.get("name_ru") or "").strip()
            if not name_original:
                continue
            tmdb_id = person.get("tmdb_id")
            person_id = _person_id(
                connection, name_original, int(tmdb_id) if str(tmdb_id).isdigit() else None, name_ru,
            )
            role = str(person.get("role") or default_role)
            interest = connection.execute(
                "SELECT 1 FROM interest_roles WHERE person_id = ? AND content_type = 'movie' AND role = ?",
                (person_id, role),
            ).fetchone()
            connection.execute(
                "INSERT OR REPLACE INTO movie_people(movie_id, person_id, credit_role, character_name, job, is_interest) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (item_id, person_id, role, str(person.get("character") or ""), str(person.get("job") or ""), int(bool(interest))),
            )


def add_item(item: dict[str, Any]) -> dict[str, Any]:
    item = dict(item)
    item.setdefault("content_type", "movie")
    item.setdefault("status", "backlog")
    item.setdefault("reaction", "")
    _validate(item)
    initialize_database()
    item_id = str(item.get("id") or f"movie-{uuid.uuid4().hex[:12]}")
    now = _now()
    status = str(item.get("status") or "backlog")
    reaction = str(item.get("reaction") or "")
    consumed_at = str(item.get("consumed_at") or now) if status == "consumed" else None
    tmdb_id = item.get("tmdb_id") or item.get("external_id")
    imdb_rating = item.get("imdb_rating")
    if imdb_rating in (None, "") and item.get("external_rating_source") == "imdb":
        imdb_rating = item.get("external_rating")
    tmdb_rating = item.get("tmdb_rating")
    if tmdb_rating in (None, "") and item.get("external_rating_source") == "tmdb":
        tmdb_rating = item.get("external_rating")
    with _lock, connect() as connection:
        if _duplicate(connection, item):
            raise StorageError("This movie is already in the library")
        connection.execute(
            "INSERT INTO content_items(id, content_type, title_ru, title_original, status, reaction, source, source_url, notes, raw_json, metadata_json, added_at, consumed_at, updated_at) "
            "VALUES (?, 'movie', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item_id, str(item.get("title_ru") or "").strip(), str(item.get("title_original") or "").strip(),
                status, reaction, str(item.get("source") or "manual"), str(item.get("url") or item.get("source_url") or ""),
                str(item.get("notes") or ""),
                _json(
                    item.get("raw_data") or {
                        key: item.get(key) for key in ("title_original", "title_ru", "year", "directors", "notes", "tmdb_id")
                        if item.get(key) not in (None, "")
                    },
                    "{}",
                ),
                _json(item.get("metadata_json"), "{}"), str(item.get("added_at") or now), consumed_at, now,
            ),
        )
        connection.execute(
            "INSERT INTO movies(content_id, release_date, release_year, runtime_minutes, imdb_rating, kinopoisk_rating, tmdb_rating, tmdb_vote_count, imdb_id, kinopoisk_id, tmdb_id, overview, original_language, awards_json, tagline, content_rating, imdb_votes, metascore, box_office, details_json, tmdb_updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item_id, item.get("release_date") or None, _year(item), item.get("duration_minutes") or item.get("runtime_minutes") or None,
                imdb_rating or None, item.get("kinopoisk_rating") or None, tmdb_rating or None,
                item.get("tmdb_vote_count") or None, item.get("imdb_id") or None,
                int(item["kinopoisk_id"]) if str(item.get("kinopoisk_id") or "").isdigit() else None,
                int(tmdb_id) if str(tmdb_id).isdigit() else None,
                str(item.get("overview") or ""), str(item.get("original_language") or ""),
                _json(item.get("awards_json"), "[]"), str(item.get("tagline") or ""),
                str(item.get("content_rating") or ""), str(item.get("imdb_votes") or ""),
                item.get("metascore") or None, str(item.get("box_office") or ""),
                _json(item.get("details_json") or _detail_payload(item), "{}"), item.get("tmdb_updated_at") or None,
            ),
        )
        _replace_movie_relations(connection, item_id, item)
        if str(tmdb_id).isdigit():
            connection.execute(
                "INSERT OR IGNORE INTO content_aliases(content_id, alias, language, provider, external_id, notes) VALUES (?, '', '', 'tmdb', ?, 'Added automatically')",
                (item_id, str(tmdb_id)),
            )
        if str(item.get("kinopoisk_id") or "").isdigit():
            connection.execute(
                "INSERT OR IGNORE INTO content_aliases(content_id, alias, language, provider, external_id, notes) VALUES (?, '', '', 'kinopoisk', ?, 'Added automatically')",
                (item_id, str(item["kinopoisk_id"])),
            )
    return get_item(item_id)


def update_item(item_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    allowed = {"status", "reaction", "notes"}
    changes = {key: value for key, value in changes.items() if key in allowed}
    if not changes:
        raise StorageError("No supported fields to update")
    _validate(changes, partial=True)
    initialize_database()
    with _lock, connect() as connection:
        row = connection.execute("SELECT status, reaction, notes, consumed_at FROM content_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            raise StorageError("Item not found")
        status = str(changes.get("status", row["status"]))
        reaction = str(changes.get("reaction", row["reaction"]) or "")
        consumed_at = row["consumed_at"]
        if status == "consumed" and not consumed_at:
            consumed_at = _now()
        if status == "backlog":
            consumed_at, reaction = None, ""
        connection.execute(
            "UPDATE content_items SET status = ?, reaction = ?, notes = ?, consumed_at = ?, updated_at = ? WHERE id = ?",
            (status, reaction, str(changes.get("notes", row["notes"])), consumed_at, _now(), item_id),
        )
    return get_item(item_id)


def update_movie_from_provider(item_id: str, details: dict[str, Any]) -> dict[str, Any]:
    initialize_database()
    with _lock, connect() as connection:
        if not connection.execute("SELECT 1 FROM movies WHERE content_id = ?", (item_id,)).fetchone():
            raise StorageError("Movie not found")
        connection.execute(
            "UPDATE content_items SET title_ru = COALESCE(NULLIF(?, ''), title_ru), title_original = COALESCE(NULLIF(?, ''), title_original), "
            "source_url = COALESCE(NULLIF(?, ''), source_url), updated_at = ? WHERE id = ?",
            (details.get("title_ru", ""), details.get("title_original", ""), details.get("url", ""), _now(), item_id),
        )
        connection.execute(
            "UPDATE movies SET release_date = ?, release_year = ?, runtime_minutes = ?, "
            "imdb_rating = COALESCE(?, imdb_rating), kinopoisk_rating = COALESCE(?, kinopoisk_rating), "
            "tmdb_rating = ?, tmdb_vote_count = ?, imdb_id = COALESCE(NULLIF(?, ''), imdb_id), "
            "kinopoisk_id = COALESCE(?, kinopoisk_id), tmdb_id = ?, overview = ?, original_language = ?, "
            "awards_json = COALESCE(?, awards_json), tagline = COALESCE(NULLIF(?, ''), tagline), "
            "content_rating = COALESCE(NULLIF(?, ''), content_rating), imdb_votes = COALESCE(NULLIF(?, ''), imdb_votes), "
            "metascore = COALESCE(?, metascore), box_office = COALESCE(NULLIF(?, ''), box_office), "
            "details_json = ?, tmdb_updated_at = ? WHERE content_id = ?",
            (
                details.get("release_date") or None, _year(details), details.get("duration_minutes") or None,
                details.get("imdb_rating") or None, details.get("kinopoisk_rating") or None,
                details.get("tmdb_rating") or None, details.get("tmdb_vote_count") or None,
                details.get("imdb_id") or "", details.get("kinopoisk_id") or None,
                details.get("tmdb_id") or None, details.get("overview") or "",
                details.get("original_language") or "", _json(details["awards_json"], "[]") if details.get("awards_json") else None,
                str(details.get("tagline") or ""), str(details.get("content_rating") or ""),
                str(details.get("imdb_votes") or ""), details.get("metascore") or None,
                str(details.get("box_office") or ""), _json(details.get("details_json") or _detail_payload(details), "{}"),
                _now(), item_id,
            ),
        )
        _replace_movie_relations(connection, item_id, details)
        if str(details.get("kinopoisk_id") or "").isdigit():
            connection.execute(
                "INSERT OR IGNORE INTO content_aliases(content_id, alias, language, provider, external_id, notes) VALUES (?, '', '', 'kinopoisk', ?, 'Added automatically')",
                (item_id, str(details["kinopoisk_id"])),
            )
    return get_item(item_id)


def movie_refresh_targets() -> list[dict[str, Any]]:
    return [
        {key: row[key] for key in ("id", "title_original", "title_ru", "year", "tmdb_id", "kinopoisk_rating")}
        for row in list_library(content_type="movie")
    ]


def known_movie_keys() -> tuple[set[str], set[str]]:
    rows = list_library(content_type="movie", include_trashed=True)
    ids = {str(row["tmdb_id"]) for row in rows if row.get("tmdb_id") != ""}
    titles: set[str] = set()
    for row in rows:
        year = str(row.get("year") or "")
        for field in ("title_original", "title_ru"):
            title = _normalized(str(row.get(field) or ""))
            if title:
                titles.add(f"{title}:{year}")
                titles.add(title)
    return ids, titles
