from __future__ import annotations

import argparse
import json
import mimetypes
import os
import threading
import urllib.request
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import llm, storage, tmdb


STATIC_DIR = Path(__file__).resolve().parent / "static"
PID_FILE = Path(__file__).resolve().parents[1] / ".runtime" / "server.pid"
APP_VERSION = 14


class Handler(BaseHTTPRequestHandler):
    server_version = "WhatsNewChecker/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[web] {self.address_string()} {fmt % args}")

    def _json(self, payload: object, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        self._json({"error": message}, status)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_000_000:
            raise ValueError("Request is too large")
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        if not isinstance(payload, dict):
            raise ValueError("JSON object expected")
        return payload

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._json({"ok": True, "version": APP_VERSION, "storage": "sqlite"})
            return
        if parsed.path == "/api/library":
            query = parse_qs(parsed.query)
            self._json(
                {
                    "items": storage.list_library(
                        content_type=query.get("content_type", [None])[0],
                        status=query.get("status", [None])[0],
                    )
                }
            )
            return
        if parsed.path == "/api/meta":
            self._json(
                {
                    "interests": storage.list_interests(),
                    "content_types": storage.list_content_types(),
                    "tmdb": tmdb.configuration(),
                    "llm": llm.configuration(),
                }
            )
            return
        if parsed.path == "/api/people":
            query = parse_qs(parsed.query)
            self._json({"items": storage.list_interests("movie", query.get("role", [None])[0])})
            return
        if parsed.path == "/api/trash":
            self._json({"items": storage.list_trash()})
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._body()
            if parsed.path == "/api/resolve/movie":
                self._json({"item": tmdb.resolve_movie_input(payload)})
                return
            if parsed.path == "/api/resolve/person":
                self._json({"item": tmdb.resolve_person_input(payload)})
                return
            if parsed.path == "/api/trash":
                self._json({"item": storage.trash_entity(payload)}, HTTPStatus.CREATED)
                return
            if parsed.path.startswith("/api/trash/") and parsed.path.endswith("/restore"):
                trash_id = unquote(parsed.path[len("/api/trash/"):-len("/restore")]).strip("/")
                if not trash_id:
                    raise ValueError("Trash item ID is required")
                self._json({"item": storage.restore_trash(trash_id)})
                return
            if parsed.path == "/api/library":
                self._json({"item": storage.add_item(payload)}, HTTPStatus.CREATED)
                return
            if parsed.path == "/api/library/refresh-tmdb":
                self._json(tmdb.refresh_library())
                return
            if parsed.path.startswith("/api/library/") and parsed.path.endswith("/refresh-tmdb"):
                item_id = unquote(parsed.path[len("/api/library/"):-len("/refresh-tmdb")]).strip("/")
                if not item_id:
                    raise ValueError("Movie ID is required")
                self._json({"item": tmdb.refresh_movie(item_id)})
                return
            if parsed.path == "/api/people":
                self._json({"item": storage.add_interest_person(payload)}, HTTPStatus.CREATED)
                return
            if parsed.path == "/api/people/refresh-tmdb":
                self._json(tmdb.refresh_people())
                return
            if parsed.path.startswith("/api/people/") and parsed.path.endswith("/refresh-tmdb"):
                person_id = unquote(parsed.path[len("/api/people/"):-len("/refresh-tmdb")]).strip("/")
                if not person_id:
                    raise ValueError("Person ID is required")
                self._json({"item": tmdb.refresh_person(person_id)})
                return
            if parsed.path == "/api/recommendations/tmdb":
                self._json({"items": tmdb.recommend_movies(payload)})
                return
            if parsed.path == "/api/recommendations/llm":
                self._json(llm.recommend_movies(payload))
                return
            self._error("Endpoint not found", HTTPStatus.NOT_FOUND)
        except (ValueError, storage.StorageError, tmdb.TmdbError, llm.LlmError) as error:
            self._error(str(error))
        except Exception as error:
            self._error(f"Internal error: {error}", HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_PATCH(self) -> None:
        parsed = urlparse(self.path)
        prefix = "/api/library/"
        if not parsed.path.startswith(prefix):
            self._error("Endpoint not found", HTTPStatus.NOT_FOUND)
            return
        try:
            item_id = unquote(parsed.path[len(prefix):])
            self._json({"item": storage.update_item(item_id, self._body())})
        except (ValueError, storage.StorageError) as error:
            self._error(str(error))

    def _serve_static(self, url_path: str) -> None:
        relative = "index.html" if url_path in ("", "/") else unquote(url_path.lstrip("/"))
        candidate = (STATIC_DIR / relative).resolve()
        try:
            candidate.relative_to(STATIC_DIR.resolve())
        except ValueError:
            self._error("Not found", HTTPStatus.NOT_FOUND)
            return
        if not candidate.is_file():
            self._error("Not found", HTTPStatus.NOT_FOUND)
            return
        body = candidate.read_bytes()
        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8" if content_type.startswith("text/") else content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _already_running(host: str, port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=0.5) as response:
            return response.status == 200
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local content library")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open-browser", action="store_true")
    args = parser.parse_args()
    url = f"http://{args.host}:{args.port}"
    if _already_running(args.host, args.port):
        if args.open_browser:
            webbrowser.open(url)
        print(f"Already running at {url}")
        return
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    if args.open_browser:
        threading.Timer(0.35, lambda: webbrowser.open(url)).start()
    print(f"Content library is available at {url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        try:
            if PID_FILE.read_text(encoding="utf-8").strip() == str(os.getpid()):
                PID_FILE.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()
