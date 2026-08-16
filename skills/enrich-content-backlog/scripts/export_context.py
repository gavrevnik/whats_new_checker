#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DB = ROOT / "data" / "library.sqlite3"


def main() -> None:
    connection = sqlite3.connect(DB)
    connection.row_factory = sqlite3.Row
    existing = [dict(row) for row in connection.execute("""
        SELECT i.id, i.title_original, i.title_ru, m.release_year AS year,
               i.status, i.reaction, m.tmdb_id
        FROM content_items i JOIN movies m ON m.content_id = i.id
        ORDER BY i.status, i.title_original
    """)]
    interests = [dict(row) for row in connection.execute("""
        SELECT p.id, ir.role, p.name_original, p.tmdb_id
        FROM people p JOIN interest_roles ir ON ir.person_id = p.id
        WHERE p.active = 1 AND ir.content_type = 'movie'
        ORDER BY ir.role, p.name_original
    """)]
    aliases = [dict(row) for row in connection.execute(
        "SELECT content_id, alias, provider, external_id FROM content_aliases ORDER BY content_id"
    )]
    print(json.dumps({"content_type":"movie","existing":existing,"interests":interests,"aliases":aliases}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
