"""Small WSGI adapter for the synthetic merchant event lab."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from .ingestion import AuthenticationError, PayloadError, SignedEventIngestor
from .inventory import InventoryStore

StartResponse = Callable[[str, list[tuple[str, str]]], Any]


class _FixedWindowRateLimiter:
    """Thread-safe, process-local protection for the demo ingestion endpoint."""

    def __init__(
        self,
        limit: int,
        window_seconds: int,
        clock: Callable[[], float],
    ):
        if type(limit) is not int or limit <= 0:
            raise ValueError("event_rate_limit must be a positive integer")
        if type(window_seconds) is not int or window_seconds <= 0:
            raise ValueError("rate_window_seconds must be a positive integer")
        self.limit = limit
        self.window_seconds = window_seconds
        self._clock = clock
        self._window_started: float | None = None
        self._requests = 0
        self._lock = threading.Lock()

    def allow(self) -> tuple[bool, int]:
        with self._lock:
            now = self._clock()
            if self._window_started is None or now - self._window_started >= self.window_seconds:
                self._window_started = now
                self._requests = 0
            elapsed = max(0.0, now - self._window_started)
            retry_after = max(1, int(self.window_seconds - elapsed + 0.999))
            if self._requests >= self.limit:
                return False, retry_after
            self._requests += 1
            return True, retry_after


class InventoryHTTPApp:
    """Expose signed event ingestion and inventory reads through WSGI.

    This adapter intentionally uses only the Python standard library. It is a
    local demonstration boundary, not a production web server.
    """

    def __init__(
        self,
        store: InventoryStore,
        secrets: Mapping[str, bytes],
        *,
        max_payload_bytes: int = 64 * 1024,
        max_age_seconds: int = 5 * 60,
        event_rate_limit: int = 60,
        rate_window_seconds: int = 60,
        clock: Callable[[], float] | None = None,
        access_log: Callable[[Mapping[str, object]], None] | None = None,
        request_id_factory: Callable[[], str] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ):
        self.store = store
        selected_clock = clock if clock is not None else time.time
        options: dict[str, Any] = {
            "max_payload_bytes": max_payload_bytes,
            "max_age_seconds": max_age_seconds,
            "clock": selected_clock,
        }
        self.ingestor = SignedEventIngestor(store, secrets, **options)
        self._event_rate_limiter = _FixedWindowRateLimiter(
            event_rate_limit,
            rate_window_seconds,
            selected_clock,
        )
        self._access_log = access_log
        self._request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)
        self._monotonic_clock = monotonic_clock or time.monotonic

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        request_id = self._request_id_factory()
        if (
            not isinstance(request_id, str)
            or not request_id
            or len(request_id) > 128
            or not request_id.isascii()
            or not request_id.isprintable()
        ):
            raise ValueError("request_id_factory must return 1-128 printable ASCII characters")
        started = self._monotonic_clock()
        response_status = 500

        def observed_start_response(status: str, headers: list[tuple[str, str]]) -> Any:
            nonlocal response_status
            response_status = int(status.partition(" ")[0])
            return start_response(status, [*headers, ("X-Request-ID", request_id)])

        response = self._dispatch(environ, observed_start_response)
        if self._access_log is not None:
            self._access_log(
                {
                    "duration_ms": round(max(0.0, self._monotonic_clock() - started) * 1000, 3),
                    "method": environ.get("REQUEST_METHOD", "").upper(),
                    "path": environ.get("PATH_INFO", ""),
                    "request_id": request_id,
                    "status": response_status,
                }
            )
        return response

    def _dispatch(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        path = environ.get("PATH_INFO", "")
        method = environ.get("REQUEST_METHOD", "").upper()

        if path == "/events":
            if method != "POST":
                return self._respond(start_response, "405 Method Not Allowed", {"error": "method not allowed"}, [("Allow", "POST")])
            return self._post_event(environ, start_response)
        if path == "/inventory":
            if method != "GET":
                return self._respond(start_response, "405 Method Not Allowed", {"error": "method not allowed"}, [("Allow", "GET")])
            return self._get_inventory(environ, start_response)
        if path == "/healthz":
            if method != "GET":
                return self._respond(start_response, "405 Method Not Allowed", {"error": "method not allowed"}, [("Allow", "GET")])
            return self._respond(start_response, "200 OK", {"status": "ok"})
        return self._respond(start_response, "404 Not Found", {"error": "not found"})

    def _post_event(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        allowed, retry_after = self._event_rate_limiter.allow()
        if not allowed:
            return self._respond(
                start_response,
                "429 Too Many Requests",
                {"error": "event request rate limit exceeded"},
                [("Retry-After", str(retry_after))],
            )

        content_type = environ.get("CONTENT_TYPE", "").partition(";")[0].strip().lower()
        if content_type != "application/json":
            return self._respond(start_response, "415 Unsupported Media Type", {"error": "content type must be application/json"})

        raw_length = environ.get("CONTENT_LENGTH", "")
        try:
            length = int(raw_length)
        except (TypeError, ValueError):
            return self._respond(start_response, "411 Length Required", {"error": "valid Content-Length required"})
        if length < 0:
            return self._respond(start_response, "400 Bad Request", {"error": "Content-Length cannot be negative"})
        if length > self.ingestor.max_payload_bytes:
            return self._respond(start_response, "413 Payload Too Large", {"error": "payload exceeds size limit"})

        body = environ["wsgi.input"].read(length)
        if len(body) != length:
            return self._respond(start_response, "400 Bad Request", {"error": "request body shorter than Content-Length"})

        try:
            applied = self.ingestor.ingest(
                body,
                environ.get("HTTP_X_EVENT_SIGNATURE", ""),
                environ.get("HTTP_X_EVENT_KEY_ID", ""),
                environ.get("HTTP_X_EVENT_TIMESTAMP", ""),
            )
        except AuthenticationError:
            return self._respond(start_response, "401 Unauthorized", {"error": "invalid signature"})
        except PayloadError as error:
            return self._respond(start_response, "400 Bad Request", {"error": str(error)})

        status = "202 Accepted" if applied else "200 OK"
        return self._respond(start_response, status, {"status": "applied" if applied else "replay"})

    def _get_inventory(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        try:
            query = parse_qs(
                environ.get("QUERY_STRING", ""),
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=3,
            )
            unknown = set(query) - {"limit", "after_sku", "low_stock_at"}
            if unknown or any(len(values) != 1 for values in query.values()):
                raise ValueError("query parameters must be unique and supported")
            parsed_limit = self._query_int(query, "limit", default=100)
            if parsed_limit is None:  # The non-None default makes this unreachable.
                raise ValueError("limit is required")
            low_stock_at = self._query_int(query, "low_stock_at", default=None)
            after_sku = query.get("after_sku", [None])[0]
            page = self.store.inventory_page(
                limit=parsed_limit,
                after_sku=after_sku,
                low_stock_at=low_stock_at,
            )
        except ValueError as error:
            return self._respond(start_response, "400 Bad Request", {"error": str(error)})

        return self._respond(
            start_response,
            "200 OK",
            {
                "items": [
                    {"sku": position.sku, "stock": position.stock}
                    for position in page.items
                ],
                "next_cursor": page.next_cursor,
            },
        )

    @staticmethod
    def _query_int(query: dict[str, list[str]], name: str, *, default: int | None) -> int | None:
        if name not in query:
            return default
        try:
            return int(query[name][0])
        except ValueError as error:
            raise ValueError(f"{name} must be an integer") from error

    @staticmethod
    def _respond(
        start_response: StartResponse,
        status: str,
        payload: dict[str, Any],
        extra_headers: list[tuple[str, str]] | None = None,
    ) -> list[bytes]:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        headers = [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store"),
        ]
        if extra_headers:
            headers.extend(extra_headers)
        start_response(status, headers)
        return [body]


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _write_access_log(event: Mapping[str, object]) -> None:
    """Write one bounded JSON record without request bodies, queries, or secrets."""

    print(json.dumps(event, separators=(",", ":"), sort_keys=True), file=sys.stderr, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local merchant-event demo API")
    parser.add_argument("--database", default="inventory.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--event-rate-limit", type=_positive_int, default=60)
    parser.add_argument("--rate-window-seconds", type=_positive_int, default=60)
    args = parser.parse_args()

    raw_secrets = os.environ.get("MERCHANT_EVENT_SECRETS")
    if not raw_secrets:
        parser.error("MERCHANT_EVENT_SECRETS must be set to a JSON object")
    try:
        decoded_secrets = json.loads(raw_secrets)
        if not isinstance(decoded_secrets, dict) or not decoded_secrets:
            raise ValueError
        secrets = {
            key_id: secret.encode()
            for key_id, secret in decoded_secrets.items()
            if isinstance(key_id, str) and isinstance(secret, str)
        }
        if len(secrets) != len(decoded_secrets):
            raise ValueError
        app = InventoryHTTPApp(
            InventoryStore(args.database),
            secrets,
            event_rate_limit=args.event_rate_limit,
            rate_window_seconds=args.rate_window_seconds,
            access_log=_write_access_log,
        )
    except (json.JSONDecodeError, UnicodeEncodeError, ValueError):
        parser.error("MERCHANT_EVENT_SECRETS must map key IDs to string secrets")
    with make_server(args.host, args.port, app) as server:
        print(f"Serving local demo on http://{args.host}:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
