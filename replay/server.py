"""A stand-in for SerpApi that answers from saved responses. Standard library only.

It serves the three endpoints the provider uses, so the example pipeline and the
tests run with no API key and spend no searches. Every body it returns carries
"_replay": true. Nothing it returns is real search data unless a captured
response for that exact query has been saved under fixtures/<engine>/.

Run it with:  python -m replay.server --port 8089
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

FIXTURES = Path(__file__).parent / "fixtures"
FAIL_QUERY = "__fail__"
_IGNORED = {"api_key", "async", "no_cache"}


def search_id_for(params: dict[str, str]) -> str:
    """The same parameters always give the same id, as SerpApi's cache would."""
    canonical = json.dumps(sorted((k, v) for k, v in params.items() if k not in _IGNORED))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:24]


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def _price_shift(query: str) -> float:
    """A whole-rupee shift between -5000 and +5000 derived from the query."""
    return (int(hashlib.sha1((query or "").encode("utf-8")).hexdigest(), 16) % 21 - 10) * 500.0


def _fill_template(body: dict[str, Any], query: str) -> dict[str, Any]:
    shift = _price_shift(query)
    body["search_parameters"]["q"] = query
    for item in body.get("shopping_results", []):
        item["title"] = str(item.get("title", "")).replace("{q}", query)
        for number, text in (("extracted_price", "price"), ("extracted_old_price", "old_price")):
            if isinstance(item.get(number), (int, float)):
                item[number] = float(item[number]) + shift
                item[text] = f"₹{item[number]:,.2f}"
    return body


def load_response(engine: str, query: str) -> dict[str, Any] | None:
    """A captured response for this exact query if one exists, else the engine's template."""
    captured = FIXTURES / engine / f"{slug(query)}.json"
    if captured.is_file():
        body = json.loads(captured.read_text(encoding="utf-8"))
        body["_replay"] = True
        return body
    template = FIXTURES / f"{engine}.json"
    if template.is_file():
        return _fill_template(copy.deepcopy(json.loads(template.read_text(encoding="utf-8"))), query)
    return None


class ReplayState:
    def __init__(self, processing_seconds: float, searches_left: int, hourly_limit: int) -> None:
        self.lock = threading.Lock()
        self.processing_seconds = processing_seconds
        self.searches_per_month = searches_left
        self.searches_left = searches_left
        self.hourly_limit = hourly_limit
        self.used_this_hour = 0
        self.searches: dict[str, dict[str, Any]] = {}

    def register(self, params: dict[str, str], ready_now: bool) -> tuple[str, bool]:
        """Record a search. Returns its id and whether it was new. A repeat is free, like a cache hit."""
        search_id = search_id_for(params)
        with self.lock:
            if search_id in self.searches:
                return search_id, False
            if self.searches_left <= 0:
                raise OutOfSearches
            self.searches_left -= 1
            self.used_this_hour += 1
            submitted = time.monotonic() - (self.processing_seconds if ready_now else 0.0)
            self.searches[search_id] = {"params": params, "submitted": submitted}
            return search_id, True

    def lookup(self, search_id: str) -> tuple[dict[str, str], bool] | None:
        with self.lock:
            entry = self.searches.get(search_id)
        if entry is None:
            return None
        ready = time.monotonic() - entry["submitted"] >= self.processing_seconds
        return entry["params"], ready


class OutOfSearches(Exception):
    pass


def _public(params: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in params.items() if key != "api_key"}


def _metadata(search_id: str, status: str) -> dict[str, Any]:
    return {"id": search_id, "status": status}


def _result_body(search_id: str, params: dict[str, str], cached: bool = False) -> dict[str, Any]:
    engine = params.get("engine", "google")
    query = params.get("q", "")
    if query == FAIL_QUERY:
        body = None
        error = "Replay: this query is set up to fail."
    else:
        body = load_response(engine, query)
        error = f"The replay server has no saved response for engine '{engine}'."
    if body is None:
        return {
            "_replay": True,
            "search_metadata": _metadata(search_id, "Error"),
            "search_parameters": _public(params),
            "error": error,
        }
    body["_replay"] = True
    body["search_metadata"] = {**body.get("search_metadata", {}), **_metadata(search_id, "Cached" if cached else "Success")}
    body["search_parameters"] = {**body.get("search_parameters", {}), **_public(params)}
    body["search_parameters"].pop("async", None)
    return body


def _make_handler(state: ReplayState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "SerpApiReplay/0.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Log the path only. The query string carries the API key parameter.
            print(f"[replay] {self.command} {urlparse(self.path).path}", flush=True)

        def _send(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            params = dict(parse_qsl(url.query, keep_blank_values=True))
            if not params.get("api_key"):
                self._send(401, {"_replay": True, "error": "Invalid API key. Replay accepts any non-empty key."})
                return

            if url.path == "/account.json":
                with state.lock:
                    self._send(
                        200,
                        {
                            "_replay": True,
                            "plan_name": "Replay",
                            "searches_per_month": state.searches_per_month,
                            "plan_searches_left": state.searches_left,
                            "total_searches_left": state.searches_left,
                            "this_month_usage": state.searches_per_month - state.searches_left,
                            "this_hour_searches": state.used_this_hour,
                            "account_rate_limit_per_hour": state.hourly_limit,
                        },
                    )
                return

            if url.path == "/search.json":
                is_async = params.get("async") == "true"
                try:
                    search_id, new = state.register(_public(params), ready_now=not is_async)
                except OutOfSearches:
                    self._send(429, {"_replay": True, "error": "Your account has run out of searches."})
                    return
                if is_async:
                    self._send(
                        200,
                        {
                            "_replay": True,
                            "search_metadata": _metadata(search_id, "Processing"),
                            "search_parameters": {k: v for k, v in _public(params).items() if k != "async"},
                        },
                    )
                else:
                    self._send(200, _result_body(search_id, _public(params), cached=not new))
                return

            match = re.fullmatch(r"/searches/([A-Za-z0-9_-]+)\.json", url.path)
            if match:
                found = state.lookup(match.group(1))
                if found is None:
                    self._send(404, {"_replay": True, "error": "Search not found."})
                    return
                stored, ready = found
                if not ready:
                    self._send(
                        200,
                        {
                            "_replay": True,
                            "search_metadata": _metadata(match.group(1), "Processing"),
                            "search_parameters": {k: v for k, v in stored.items() if k != "async"},
                        },
                    )
                else:
                    self._send(200, _result_body(match.group(1), stored))
                return

            self._send(404, {"_replay": True, "error": "Not found."})

    return Handler


def make_server(
    host: str = "127.0.0.1",
    port: int = 0,
    processing_seconds: float = 4.0,
    searches_left: int = 250,
    hourly_limit: int = 50,
) -> ThreadingHTTPServer:
    """Build the server without starting it. `server.state` holds its counters."""
    state = ReplayState(processing_seconds, searches_left, hourly_limit)
    server = ThreadingHTTPServer((host, port), _make_handler(state))
    server.state = state  # type: ignore[attr-defined]
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay server standing in for SerpApi.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8089)
    parser.add_argument(
        "--processing-seconds", type=float, default=float(os.environ.get("REPLAY_PROCESSING_SECONDS", "4"))
    )
    parser.add_argument("--searches-left", type=int, default=int(os.environ.get("REPLAY_SEARCHES_LEFT", "250")))
    parser.add_argument("--hourly-limit", type=int, default=int(os.environ.get("REPLAY_HOURLY_LIMIT", "50")))
    args = parser.parse_args()

    server = make_server(args.host, args.port, args.processing_seconds, args.searches_left, args.hourly_limit)
    print(f"[replay] serving saved responses on http://{args.host}:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
