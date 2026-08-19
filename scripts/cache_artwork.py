from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import artwork, storage


def _cache(item: dict, refresh_existing: bool = False) -> tuple[str, str, str]:
    content_type = str(item.get("content_type") or "")
    try:
        if content_type == "music":
            relative = artwork.cache_album_cover(
                str(item.get("release_group_mbid") or ""), str(item.get("cover_url") or ""),
                force=refresh_existing,
            )
        else:
            if not item.get("tmdb_id") or not (item.get("poster_path") or item.get("poster_url")):
                return str(item["id"]), "skipped", "нет TMDB ID или poster_path"
            relative = artwork.cache_movie_poster(
                item.get("tmdb_id"), str(item.get("poster_path") or ""),
                str(item.get("poster_url") or ""), force=refresh_existing,
            )
        if not relative:
            return str(item["id"]), "skipped", "обложка отсутствует у провайдера"
        stored_relative = str(
            (item.get("cover_path") if content_type == "music" else item.get("poster_local_path"))
            or ""
        )
        if relative != stored_relative:
            storage.update_artwork_path(str(item["id"]), content_type, relative)
        return str(item["id"]), "cached", relative
    except Exception as error:
        return str(item["id"]), "failed", str(error)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cache movie posters and album covers locally")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--refresh-existing", action="store_true",
        help="Atomically replace existing cache files with the current smaller provider sizes",
    )
    args = parser.parse_args()
    items = [
        *storage.list_library(content_type="movie", include_trashed=True),
        *storage.list_library(content_type="music", include_trashed=True),
    ]
    results = []
    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 12))) as executor:
        futures = [executor.submit(_cache, item, args.refresh_existing) for item in items]
        for future in as_completed(futures):
            results.append(future.result())
    cached = sum(status == "cached" for _, status, _ in results)
    skipped = sum(status == "skipped" for _, status, _ in results)
    failed = [(item_id, message) for item_id, status, message in results if status == "failed"]
    print(f"Cached: {cached}; skipped: {skipped}; failed: {len(failed)}")
    for item_id, message in failed[:20]:
        print(f"- {item_id}: {message}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
