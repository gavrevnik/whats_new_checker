from __future__ import annotations

import mimetypes
import os
import re
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ARTWORK_DIR = ROOT / "data" / "artwork"
USER_AGENT = "whats-new-checker/2.1 (gavrevns@gmail.com)"
MAX_IMAGE_BYTES = 15 * 1024 * 1024
ALBUM_COVER_SIZE = 250
MOVIE_POSTER_SIZE = "w185"


class ArtworkError(RuntimeError):
    pass


def album_cover_url(release_group_mbid: str) -> str:
    mbid = str(release_group_mbid or "").strip()
    return f"https://coverartarchive.org/release-group/{mbid}/front-{ALBUM_COVER_SIZE}" if mbid else ""


def movie_poster_url(poster_path: str) -> str:
    path = str(poster_path or "").strip()
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        if "image.tmdb.org/t/p/" in path:
            return re.sub(r"(/t/p/)[^/]+/", rf"\g<1>{MOVIE_POSTER_SIZE}/", path, count=1)
        return path
    return f"https://image.tmdb.org/t/p/{MOVIE_POSTER_SIZE}/{path.lstrip('/')}"


def preferred_album_cover_url(release_group_mbid: str, cover_url: str = "") -> str:
    url = str(cover_url or "").strip()
    if "coverartarchive.org/release-group/" in url:
        return re.sub(r"/front(?:-(?:250|500|1200))?$", f"/front-{ALBUM_COVER_SIZE}", url)
    return url or album_cover_url(release_group_mbid)


def _safe_identifier(value: object) -> str:
    normalized = "".join(character for character in str(value or "") if character.isalnum() or character in "-_")
    if not normalized:
        raise ArtworkError("Не удалось определить идентификатор изображения")
    return normalized


def _relative_path(kind: str, identifier: object) -> str:
    folder = "albums" if kind == "album" else "movies"
    return f"{folder}/{_safe_identifier(identifier)}.jpg"


def resolve_local_path(relative_path: str) -> Path:
    candidate = (ARTWORK_DIR / str(relative_path or "").lstrip("/")).resolve()
    try:
        candidate.relative_to(ARTWORK_DIR.resolve())
    except ValueError as error:
        raise ArtworkError("Недопустимый путь изображения") from error
    return candidate


def is_cached(relative_path: object) -> bool:
    if not relative_path:
        return False
    try:
        return resolve_local_path(str(relative_path)).is_file()
    except ArtworkError:
        return False


def delete_cached(relative_path: object) -> bool:
    """Delete one cached artwork file without allowing paths outside ARTWORK_DIR."""
    if not relative_path:
        return False
    candidate = resolve_local_path(str(relative_path))
    try:
        candidate.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ArtworkError(f"Не удалось удалить локальное изображение: {error}") from error


def _download(url: str, destination: Path) -> bool:
    if not url:
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    temporary_name = ""
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            content_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0]
            if content_type and not content_type.startswith("image/"):
                raise ArtworkError(f"Источник вернул не изображение ({content_type})")
            body = response.read(MAX_IMAGE_BYTES + 1)
            if len(body) > MAX_IMAGE_BYTES:
                raise ArtworkError("Файл изображения превышает допустимый размер")
        handle, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
        with os.fdopen(handle, "wb") as stream:
            stream.write(body)
        os.replace(temporary_name, destination)
        return True
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 404:
            return False
        raise ArtworkError(f"Не удалось загрузить изображение: HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise ArtworkError(f"Не удалось загрузить изображение: {error}") from error
    finally:
        if temporary_name:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError:
                pass


def cache_album_cover(release_group_mbid: str, cover_url: str = "", *, force: bool = False) -> str:
    relative = _relative_path("album", release_group_mbid)
    destination = resolve_local_path(relative)
    if destination.is_file() and not force:
        return relative
    return relative if _download(preferred_album_cover_url(release_group_mbid, cover_url), destination) else ""


def cache_movie_poster(
    tmdb_id: object, poster_path: str = "", poster_url: str = "", *, force: bool = False,
) -> str:
    relative = _relative_path("movie", tmdb_id)
    destination = resolve_local_path(relative)
    if destination.is_file() and not force:
        return relative
    return relative if _download(movie_poster_url(poster_url or poster_path), destination) else ""


def content_type(relative_path: str) -> str:
    try:
        header = resolve_local_path(relative_path).read_bytes()[:12]
    except OSError:
        header = b""
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    return mimetypes.guess_type(relative_path)[0] or "image/jpeg"
