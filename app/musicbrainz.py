from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from typing import Any

from app import artwork, listenbrainz, recommendation_progress, storage


BASE_URL = "https://musicbrainz.org/ws/2"
COVER_ART_BASE_URL = "https://coverartarchive.org"
APP_NAME = "WhatsNewChecker"
APP_VERSION = "2.0"
CONTACT_EMAIL = "gavrevns@gmail.com"
USER_AGENT = f"{APP_NAME}/{APP_VERSION} ({CONTACT_EMAIL})"
REQUEST_INTERVAL_SECONDS = 1.05
_rate_lock = threading.Lock()
_last_request_started = 0.0


class MusicBrainzError(RuntimeError):
    pass


def configuration() -> dict[str, Any]:
    return {
        "configured": True,
        "provider": "MusicBrainz Web Service API v2",
        "authentication": "public read-only",
        "rate_limit": "1 request/sec",
        "user_agent": USER_AGENT,
    }


def _wait_for_rate_limit() -> None:
    global _last_request_started
    with _rate_lock:
        remaining = REQUEST_INTERVAL_SECONDS - (time.monotonic() - _last_request_started)
        if remaining > 0:
            time.sleep(remaining)
        _last_request_started = time.monotonic()


def _request_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = {key: value for key, value in (params or {}).items() if value not in (None, "")}
    query["fmt"] = "json"
    url = f"{BASE_URL}/{path.lstrip('/')}?{urllib.parse.urlencode(query)}"
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    last_error: Exception | None = None
    for attempt in range(3):
        _wait_for_rate_limit()
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=25
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise MusicBrainzError("MusicBrainz вернул ответ неожиданного формата")
            return payload
        except urllib.error.HTTPError as error:
            last_error = error
            if error.code not in {429, 500, 502, 503, 504} or attempt == 2:
                detail = ""
                try:
                    parsed = json.loads(error.read().decode("utf-8"))
                    detail = str(parsed.get("error") or "") if isinstance(parsed, dict) else ""
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
                suffix = f": {detail}" if detail else ""
                raise MusicBrainzError(f"MusicBrainz: HTTP {error.code}{suffix}") from error
            retry_after = error.headers.get("Retry-After") if error.headers else None
            try:
                delay = max(REQUEST_INTERVAL_SECONDS, float(retry_after or 0))
            except ValueError:
                delay = REQUEST_INTERVAL_SECONDS * (attempt + 1)
            time.sleep(min(delay, 10))
        except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
            last_error = error
            if attempt == 2:
                reason = getattr(error, "reason", error)
                raise MusicBrainzError(f"MusicBrainz недоступен: {reason}") from error
            time.sleep(REQUEST_INTERVAL_SECONDS * (attempt + 1))
        except json.JSONDecodeError as error:
            raise MusicBrainzError("MusicBrainz вернул повреждённый JSON") from error
    raise MusicBrainzError(f"MusicBrainz недоступен: {last_error}")


def _cover_art_url(release_group_mbid: str) -> str:
    mbid = str(release_group_mbid or "").strip()
    if not mbid:
        return ""
    url = f"{COVER_ART_BASE_URL}/release-group/{urllib.parse.quote(mbid)}"
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        error.close()
        if error.code in {400, 404}:
            return ""
        raise MusicBrainzError(f"Cover Art Archive: HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
        reason = getattr(error, "reason", error)
        raise MusicBrainzError(f"Cover Art Archive недоступен: {reason}") from error
    except json.JSONDecodeError as error:
        raise MusicBrainzError("Cover Art Archive вернул повреждённый JSON") from error
    if not isinstance(payload, dict):
        return ""
    for image in payload.get("images", []) or []:
        if not isinstance(image, dict) or image.get("front") is not True:
            continue
        thumbnails = image.get("thumbnails") if isinstance(image.get("thumbnails"), dict) else {}
        if thumbnails.get("500") or thumbnails.get("large"):
            cover_url = f"{COVER_ART_BASE_URL}/release-group/{mbid}/front-500"
            try:
                with urllib.request.urlopen(
                    urllib.request.Request(cover_url, headers={"User-Agent": USER_AGENT}, method="HEAD"),
                    timeout=20,
                ):
                    return cover_url
            except urllib.error.HTTPError as error:
                error.close()
                if error.code == 404:
                    return ""
                raise MusicBrainzError(f"Cover Art Archive: HTTP {error.code}") from error
            except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
                reason = getattr(error, "reason", error)
                raise MusicBrainzError(f"Cover Art Archive недоступен: {reason}") from error
    return ""


def _lucene(value: str) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def _score(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _artist_payload(raw: dict[str, Any]) -> dict[str, Any]:
    area_raw = raw.get("area") if isinstance(raw.get("area"), dict) else {}
    begin_area = raw.get("begin-area") if isinstance(raw.get("begin-area"), dict) else {}
    life_span = raw.get("life-span") if isinstance(raw.get("life-span"), dict) else {}
    name = str(raw.get("name") or "").strip()
    mbid = str(raw.get("id") or raw.get("mbid") or "").strip()
    return {
        "content_type": "music",
        "role": "artist",
        "name": name,
        "name_original": name,
        "name_ru": name,
        "sort_name": str(raw.get("sort-name") or raw.get("sort_name") or name),
        "mbid": mbid,
        "external_id": mbid,
        "artist_type": str(raw.get("type") or ""),
        "country": str(raw.get("country") or ""),
        "area": str(area_raw.get("name") or begin_area.get("name") or ""),
        "disambiguation": str(raw.get("disambiguation") or ""),
        "life_span_begin": str(life_span.get("begin") or ""),
        "life_span_end": str(life_span.get("end") or ""),
        "details_json": raw,
        "musicbrainz_link": f"https://musicbrainz.org/artist/{mbid}" if mbid else "",
    }


def search_artist(name: str) -> dict[str, Any]:
    name = str(name or "").strip()
    if not name:
        raise MusicBrainzError("Укажите имя исполнителя")
    response = _request_json("artist", {"query": f"artist:{_lucene(name)}", "limit": 10})
    artists = [item for item in response.get("artists", []) if isinstance(item, dict)]
    if not artists:
        raise MusicBrainzError(f"MusicBrainz не нашёл исполнителя «{name}»")
    normalized = storage._normalized(name)
    artists.sort(
        key=lambda item: (
            storage._normalized(str(item.get("name") or "")) == normalized,
            _score(item.get("score")),
        ),
        reverse=True,
    )
    return _artist_payload(artists[0])


def artist_details(mbid: str) -> dict[str, Any]:
    if not str(mbid or "").strip():
        raise MusicBrainzError("MusicBrainz ID исполнителя не указан")
    raw = _request_json(
        f"artist/{mbid}", {"inc": "aliases+genres+tags+ratings+url-rels"}
    )
    return _artist_payload(raw)


def resolve_artist_input(payload: dict[str, Any]) -> dict[str, Any]:
    mbid = str(payload.get("mbid") or payload.get("external_id") or "").strip()
    if mbid:
        return artist_details(mbid)
    found = search_artist(
        str(payload.get("name") or payload.get("name_original") or payload.get("name_ru") or "")
    )
    return artist_details(found["mbid"])


def _artist_credits(raw: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for credit in raw.get("artist-credit", []) or []:
        if not isinstance(credit, dict):
            continue
        artist = credit.get("artist") if isinstance(credit.get("artist"), dict) else {}
        name = str(credit.get("name") or artist.get("name") or "").strip()
        if not name:
            continue
        results.append({
            "name": name,
            "credit_name": name,
            "mbid": str(artist.get("id") or ""),
            "sort_name": str(artist.get("sort-name") or name),
        })
    return results


def _named_values(raw: Any, maximum: int = 20) -> list[dict[str, Any]]:
    values = [item for item in (raw or []) if isinstance(item, dict) and item.get("name")]
    values.sort(key=lambda item: int(item.get("count") or 0), reverse=True)
    return [
        {"name": str(item["name"]), "count": int(item.get("count") or 0)}
        for item in values[:maximum]
    ]


def _release_group_payload(raw: dict[str, Any]) -> dict[str, Any]:
    title = str(raw.get("title") or "").strip()
    mbid = str(raw.get("id") or "").strip()
    first_date = str(raw.get("first-release-date") or "")
    artists = _artist_credits(raw)
    return {
        "content_type": "music",
        "title_original": title,
        "title_ru": title,
        "release_group_mbid": mbid,
        "mbid": mbid,
        "first_release_date": first_date,
        "release_date": first_date,
        "year": int(first_date[:4]) if first_date[:4].isdigit() else None,
        "primary_type": str(raw.get("primary-type") or "Album"),
        "secondary_types": [str(value) for value in (raw.get("secondary-types") or [])],
        "disambiguation": str(raw.get("disambiguation") or ""),
        "genres_data": _named_values(raw.get("genres")),
        "tags_data": _named_values(raw.get("tags")),
        "artists_data": artists,
        "artists": "; ".join(artist["credit_name"] for artist in artists),
        "musicbrainz_link": f"https://musicbrainz.org/release-group/{mbid}" if mbid else "",
        "source": "musicbrainz",
        "url": f"https://musicbrainz.org/release-group/{mbid}" if mbid else "",
    }


def search_album(title: str, artist: str = "", year: Any = None) -> dict[str, Any]:
    title = str(title or "").strip()
    artist = str(artist or "").strip()
    if not title:
        raise MusicBrainzError("Укажите название альбома")
    terms = [f"releasegroup:{_lucene(title)}", "primarytype:album"]
    if artist:
        terms.append(f"artist:{_lucene(artist)}")
    if str(year or "").isdigit():
        terms.append(f"firstreleasedate:{int(year)}")
    response = _request_json(
        "release-group", {"query": " AND ".join(terms), "limit": 10}
    )
    groups = [item for item in response.get("release-groups", []) if isinstance(item, dict)]
    if not groups and str(year or "").isdigit():
        terms = [term for term in terms if not term.startswith("firstreleasedate:")]
        response = _request_json(
            "release-group", {"query": " AND ".join(terms), "limit": 10}
        )
        groups = [item for item in response.get("release-groups", []) if isinstance(item, dict)]
    if not groups:
        suffix = f" — {artist}" if artist else ""
        raise MusicBrainzError(f"MusicBrainz не нашёл альбом «{title}{suffix}»")
    normalized_title = storage._normalized(title)
    normalized_artist = storage._normalized(artist)

    def rank(item: dict[str, Any]) -> tuple[int, int, int, int]:
        payload = _release_group_payload(item)
        candidate_year = payload.get("year")
        year_distance = abs(int(year) - int(candidate_year)) if str(year or "").isdigit() and candidate_year else 9999
        artist_match = not normalized_artist or normalized_artist in storage._normalized(payload.get("artists", ""))
        return (
            int(storage._normalized(str(item.get("title") or "")) == normalized_title),
            int(artist_match),
            -year_distance,
            _score(item.get("score")),
        )

    groups.sort(key=rank, reverse=True)
    return _release_group_payload(groups[0])


def _best_release(releases: list[dict[str, Any]], first_date: str) -> dict[str, Any] | None:
    valid = [release for release in releases if isinstance(release, dict) and release.get("id")]
    if not valid:
        return None
    status_priority = {"Official": 3, "Promotion": 2, "": 1}

    def rank(release: dict[str, Any]) -> tuple[int, int, int]:
        release_date = str(release.get("date") or "")
        same_date = int(bool(first_date and release_date == first_date))
        same_year = int(bool(first_date[:4] and release_date[:4] == first_date[:4]))
        return (same_date, same_year, status_priority.get(str(release.get("status") or ""), 0))

    return max(valid, key=rank)


def _release_details(mbid: str) -> dict[str, Any]:
    raw = _request_json(
        f"release/{mbid}",
        {"inc": "artist-credits+labels+media+recordings+release-groups"},
    )
    media = [medium for medium in raw.get("media", []) if isinstance(medium, dict)]
    tracks: list[dict[str, Any]] = []
    track_count = 0
    formats: list[str] = []
    for medium in media:
        if medium.get("format") and str(medium["format"]) not in formats:
            formats.append(str(medium["format"]))
        medium_tracks = [track for track in medium.get("tracks", []) if isinstance(track, dict)]
        track_count += int(medium.get("track-count") or len(medium_tracks))
        for track in medium_tracks:
            tracks.append({
                "number": str(track.get("number") or track.get("position") or ""),
                "title": str(track.get("title") or ""),
                "length_ms": track.get("length"),
            })
    label_info = [item for item in raw.get("label-info", []) if isinstance(item, dict)]
    labels = []
    catalog_numbers = []
    for item in label_info:
        label = item.get("label") if isinstance(item.get("label"), dict) else {}
        if label.get("name") and str(label["name"]) not in labels:
            labels.append(str(label["name"]))
        if item.get("catalog-number") and str(item["catalog-number"]) not in catalog_numbers:
            catalog_numbers.append(str(item["catalog-number"]))
    return {
        "primary_release_mbid": str(raw.get("id") or mbid),
        "track_count": track_count or None,
        "country": str(raw.get("country") or ""),
        "label": "; ".join(labels),
        "catalog_number": "; ".join(catalog_numbers),
        "barcode": str(raw.get("barcode") or ""),
        "media_formats": "; ".join(formats),
        "release_status": str(raw.get("status") or ""),
        "release_title": str(raw.get("title") or ""),
        "track_list": tracks,
    }


def album_details(
    mbid: str, *, fetch_popularity: bool = True, progress_id: object = None
) -> dict[str, Any]:
    mbid = str(mbid or "").strip()
    if not mbid:
        raise MusicBrainzError("MusicBrainz ID альбома не указан")
    try:
        raw = _request_json(
            f"release-group/{mbid}",
            {"inc": "artists+releases+genres+tags"},
        )
        payload = _release_group_payload(raw)
        best_release = _best_release(
            [item for item in raw.get("releases", []) if isinstance(item, dict)],
            str(raw.get("first-release-date") or ""),
        )
        release_details: dict[str, Any] = {}
        if best_release:
            release_details = _release_details(str(best_release["id"]))
            payload.update({key: value for key, value in release_details.items() if key != "track_list"})
        payload["details_json"] = {
            "track_list": release_details.get("track_list", []),
            "release_status": release_details.get("release_status", ""),
            "release_title": release_details.get("release_title", ""),
            "annotation": str(raw.get("annotation") or ""),
        }
    finally:
        recommendation_progress.advance(progress_id, "musicbrainz-details")
    try:
        payload["cover_url"] = _cover_art_url(mbid)
        payload["cover_art_checked"] = True
        payload["cover_path"] = (
            artwork.cache_album_cover(mbid, payload["cover_url"])
            if payload["cover_url"] else ""
        )
        payload["cover_cache_checked"] = True
    except artwork.ArtworkError as error:
        payload["cover_cache_checked"] = False
        payload.setdefault("provider_warnings", []).append({
            "provider": "cover-art-archive",
            "message": str(error),
        })
        recommendation_progress.add_warning(progress_id, "Cover Art Archive", str(error))
    except MusicBrainzError as error:
        payload["cover_url"] = ""
        payload["cover_path"] = ""
        payload["cover_art_checked"] = False
        payload["cover_cache_checked"] = False
        payload.setdefault("provider_warnings", []).append({
            "provider": "cover-art-archive",
            "message": str(error),
        })
        recommendation_progress.add_warning(progress_id, "Cover Art Archive", str(error))
    finally:
        recommendation_progress.advance(progress_id, "cover-art")
    if fetch_popularity:
        try:
            listenbrainz.enrich_albums([payload])
        except listenbrainz.ListenBrainzError as error:
            payload.setdefault("provider_warnings", []).append({
                "provider": "listenbrainz",
                "message": str(error),
            })
    return payload


def resolve_album_input(
    payload: dict[str, Any], *, fetch_popularity: bool = True
) -> dict[str, Any]:
    mbid = str(
        payload.get("release_group_mbid") or payload.get("mbid") or payload.get("external_id") or ""
    ).strip()
    if not mbid:
        found = search_album(
            str(payload.get("title_original") or payload.get("title_ru") or ""),
            str(payload.get("artists") or payload.get("artist") or ""),
            payload.get("year"),
        )
        mbid = found["release_group_mbid"]
    return album_details(mbid, fetch_popularity=fetch_popularity)


def refresh_album(item_id: str) -> dict[str, Any]:
    target = storage.get_item(item_id)
    if target.get("content_type") != "music":
        raise MusicBrainzError("Album not found")
    needs_refresh = (
        not target.get("artists")
        or not artwork.is_cached(target.get("cover_path"))
        or target.get("total_listen_count") in (None, "")
    )
    warnings: list[dict[str, str]] = []
    item = target
    if not needs_refresh:
        item["refresh_skipped"] = True
        return item
    details = resolve_album_input(target, fetch_popularity=False)
    item = storage.update_album_from_provider(item_id, details)
    warnings.extend(details.get("provider_warnings", []))
    mbid = str(item.get("release_group_mbid") or "")
    if mbid:
        try:
            counts = listenbrainz.release_group_popularity([mbid])
            storage.update_album_popularity(counts)
            item = storage.get_item(item_id)
        except listenbrainz.ListenBrainzError as error:
            warnings.append({"provider": "listenbrainz", "message": str(error)})
    if warnings:
        item["provider_warnings"] = warnings
    return item


def refresh_library() -> dict[str, Any]:
    targets = [
        target for target in storage.album_refresh_targets()
        if not target.get("artists") or not artwork.is_cached(target.get("cover_path"))
        or target.get("total_listen_count") in (None, "")
    ]
    updated_ids: set[str] = set()
    errors: list[dict[str, str]] = []
    provider_warnings: list[dict[str, str]] = []
    for target in targets:
        try:
            details = resolve_album_input(target, fetch_popularity=False)
            storage.update_album_from_provider(str(target["id"]), details)
            provider_warnings.extend(details.get("provider_warnings") or [])
            updated_ids.add(str(target["id"]))
        except Exception as error:
            errors.append({"title": str(target.get("title_original") or "Album"), "error": str(error)})
    popularity_targets = [storage.get_item(str(target["id"])) for target in targets]
    popularity_mbids = [
        str(item.get("release_group_mbid") or "") for item in popularity_targets
        if item.get("release_group_mbid")
    ]
    if popularity_mbids:
        try:
            counts = listenbrainz.release_group_popularity(popularity_mbids)
            storage.update_album_popularity(counts)
            updated_ids.update(
                str(item["id"]) for item in popularity_targets
                if item.get("release_group_mbid") in counts
            )
        except listenbrainz.ListenBrainzError as error:
            provider_warnings.append({"provider": "listenbrainz", "message": str(error)})
    return {
        "total": len(targets), "updated": len(updated_ids), "failed": len(errors), "errors": errors,
        "provider_warnings": provider_warnings + ([
            {"provider": "musicbrainz", "message": f"Не удалось обновить {len(errors)} альбомов из MusicBrainz."}
        ] if errors else []),
    }


def refresh_artist(artist_id: str) -> dict[str, Any]:
    target = next(
        (artist for artist in storage.list_music_artists() if artist["id"] == artist_id), None
    )
    if not target:
        raise MusicBrainzError("Artist not found")
    details = resolve_artist_input(target)
    return storage.update_music_artist(artist_id, details)


def refresh_artists() -> dict[str, Any]:
    targets = storage.artist_refresh_targets()
    updated = 0
    errors: list[dict[str, str]] = []
    for target in targets:
        try:
            refresh_artist(str(target["id"]))
            updated += 1
        except Exception as error:
            errors.append({"title": str(target.get("name_original") or "Artist"), "error": str(error)})
    return {"total": len(targets), "updated": updated, "failed": len(errors), "errors": errors}


def browse_artist_albums(
    artist_mbid: str,
    year_from: int,
    year_to: int,
    *,
    studio_albums_only: bool = False,
) -> list[dict[str, Any]]:
    response = _request_json(
        "release-group",
        {
            "artist": artist_mbid,
            "type": "album",
            "release-group-status": "website-default",
            "inc": "artist-credits+genres+tags",
            "limit": 100,
        },
    )
    results: list[dict[str, Any]] = []
    for raw in response.get("release-groups", []) or []:
        if not isinstance(raw, dict):
            continue
        item = _release_group_payload(raw)
        item_year = item.get("year")
        if item.get("primary_type").casefold() != "album".casefold():
            continue
        if studio_albums_only and item.get("secondary_types"):
            continue
        if not item_year or not year_from <= int(item_year) <= year_to:
            continue
        results.append(item)
    return results


def _int_filter(payload: dict[str, Any], key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(payload.get(key) if payload.get(key) not in (None, "") else default)
    except (TypeError, ValueError) as error:
        raise MusicBrainzError(f"Некорректное значение {key}") from error
    if not minimum <= value <= maximum:
        raise MusicBrainzError(f"{key} должно быть от {minimum} до {maximum}")
    return value


def recommend_albums(filters: dict[str, Any]) -> dict[str, Any]:
    current_year = date.today().year
    year_from = _int_filter(filters, "year_from", 1900, 1900, current_year + 2)
    year_to = _int_filter(filters, "year_to", current_year + 2, 1900, current_year + 2)
    limit = _int_filter(filters, "limit", 20, 1, 50)
    if year_from > year_to:
        raise MusicBrainzError("Начальный год не может быть больше конечного")
    excluded = {
        str(value).strip().casefold()
        for value in (filters.get("excluded_types") or [])
        if str(value).strip()
    }
    selected_ids = {str(value) for value in (filters.get("artist_ids") or [])}
    artists = storage.list_music_artists()
    if "artist_ids" in filters:
        artists = [artist for artist in artists if artist["id"] in selected_ids]
    artists = [artist for artist in artists if artist.get("mbid")]
    if not artists:
        raise MusicBrainzError("Нет выбранных исполнителей с MusicBrainz ID")
    progress_id = filters.get("progress_id")
    recommendation_progress.start(
        progress_id, len(artists), stage_id="musicbrainz-artists",
        label="MusicBrainz · альбомы исполнителей", unit="исполнителей",
    )
    known_ids, known_titles = storage.known_album_keys()
    candidates: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for artist in artists:
        try:
            for item in browse_artist_albums(str(artist["mbid"]), year_from, year_to):
                mbid = str(item.get("release_group_mbid") or "")
                normalized = storage._normalized(str(item.get("title_original") or ""))
                secondary = {str(value).casefold() for value in item.get("secondary_types", [])}
                if not mbid or mbid in known_ids or normalized in known_titles or secondary & excluded:
                    continue
                candidates.setdefault(mbid, item)
        except Exception as error:
            errors.append({"title": str(artist.get("name_original") or "Artist"), "error": str(error)})
            recommendation_progress.add_warning(progress_id, "MusicBrainz", str(error))
        finally:
            recommendation_progress.advance(progress_id, "musicbrainz-artists")
    recommendation_progress.finish_stage(progress_id, "musicbrainz-artists")
    ordered = sorted(
        candidates.values(),
        key=lambda item: int(item.get("year") or 0),
        reverse=True,
    )
    selected = ordered[:limit]
    recommendation_progress.set_stage(
        progress_id, "musicbrainz-details", "MusicBrainz · карточки альбомов",
        len(selected), "альбомов",
    )
    recommendation_progress.set_stage(
        progress_id, "cover-art", "Cover Art Archive · обложки",
        len(selected), "альбомов",
    )
    items: list[dict[str, Any]] = []
    for candidate in selected:
        try:
            items.append(album_details(
                str(candidate["release_group_mbid"]), fetch_popularity=False,
                progress_id=progress_id,
            ))
        except Exception as error:
            errors.append({"title": str(candidate.get("title_original") or "Album"), "error": str(error)})
            recommendation_progress.advance(progress_id, "cover-art")
            recommendation_progress.add_warning(progress_id, "MusicBrainz", str(error))
    recommendation_progress.finish_stage(progress_id, "musicbrainz-details")
    recommendation_progress.finish_stage(progress_id, "cover-art")
    warnings: list[dict[str, str]] = []
    if items:
        batch_total = (len(items) + listenbrainz.BATCH_SIZE - 1) // listenbrainz.BATCH_SIZE
        recommendation_progress.set_stage(
            progress_id, "listenbrainz", "ListenBrainz · прослушивания",
            batch_total, "пакетов",
        )

        def popularity_error(error: listenbrainz.ListenBrainzError) -> None:
            warning = {"provider": "listenbrainz", "message": str(error)}
            if warning not in warnings:
                warnings.append(warning)
            recommendation_progress.add_warning(progress_id, "ListenBrainz", str(error))

        listenbrainz.enrich_albums(
            items,
            continue_on_error=True,
            on_batch=lambda: recommendation_progress.advance(progress_id, "listenbrainz"),
            on_error=popularity_error,
        )
        recommendation_progress.finish_stage(progress_id, "listenbrainz")
    else:
        recommendation_progress.set_stage(
            progress_id, "listenbrainz", "ListenBrainz · прослушивания", 0, "пакетов"
        )
        recommendation_progress.finish_stage(progress_id, "listenbrainz")
    if errors:
        warnings.append({
            "provider": "musicbrainz",
            "message": f"MusicBrainz вернул неполные данные: пропущено {len(errors)} позиций.",
        })
    item_warnings = [
        warning for item in items for warning in item.get("provider_warnings", [])
        if isinstance(warning, dict)
    ]
    combined_warnings: list[dict[str, str]] = []
    for warning in [*item_warnings, *warnings]:
        if warning not in combined_warnings:
            combined_warnings.append(warning)
    for item in items:
        item["provider_warnings"] = item.get("provider_warnings", []) + warnings
    recommendation_progress.finish(progress_id)
    return {"items": items, "errors": errors, "provider_warnings": combined_warnings}
