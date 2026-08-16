#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from app.storage import StorageError, add_item


def main() -> None:
    parser = argparse.ArgumentParser(description="Add a normalized movie to SQLite")
    parser.add_argument("--title-original", required=True)
    parser.add_argument("--title-ru", required=True)
    parser.add_argument("--status", choices=["backlog", "consumed", "dismissed"], default="backlog")
    parser.add_argument("--reaction", choices=["like", "dislike"], default="")
    parser.add_argument("--year", default="")
    parser.add_argument("--release-date", default="")
    parser.add_argument("--director", action="append", default=[])
    parser.add_argument("--genre", action="append", default=[])
    parser.add_argument("--runtime", default="")
    parser.add_argument("--imdb-rating", default="")
    parser.add_argument("--tmdb-id", default="")
    parser.add_argument("--imdb-id", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    item = {
        "content_type":"movie", "title_original":args.title_original, "title_ru":args.title_ru,
        "status":args.status, "reaction":args.reaction, "year":args.year, "release_date":args.release_date,
        "directors":"; ".join(args.director), "genres":"; ".join(args.genre),
        "duration_minutes":args.runtime, "imdb_rating":args.imdb_rating, "tmdb_id":args.tmdb_id,
        "imdb_id":args.imdb_id, "notes":args.notes, "source":"manual",
    }
    if args.dry_run:
        print(json.dumps(item, ensure_ascii=False, indent=2)); return
    try:
        print(json.dumps(add_item(item), ensure_ascii=False, indent=2))
    except StorageError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
