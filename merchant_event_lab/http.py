"""Small WSGI adapter for the synthetic merchant event lab."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
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

_TRACEPARENT = re.compile(
    r"^00-(?P<trace_id>[0-9a-f]{32})-(?P<parent_id>[0-9a-f]{16})-(?P<flags>0[01])$"
)


def _parse_traceparent(value: object) -> tuple[str, str, str] | None:
    """Accept the narrow W3C version-00 trace context used by this demo."""

    if not isinstance(value, str):
        return None
    match = _TRACEPARENT.fullmatch(value)
    if match is None:
        return None
    trace_id = match.group("trace_id")
    parent_id = match.group("parent_id")
    if trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return trace_id, parent_id, match.group("flags")


def _validated_hex_id(value: str, length: int, factory_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
        or value == "0" * length
    ):
        raise ValueError(
            f"{factory_name} must return {length} lowercase non-zero hex characters"
        )
    return value


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


class _RequestMetrics:
    """Thread-safe, bounded-label request metrics for the local demo."""

    def __init__(self) -> None:
        self._values: dict[tuple[str, str, int], tuple[int, float]] = {}
        self._lock = threading.Lock()

    def observe(self, method: str, route: str, status: int, duration_seconds: float) -> None:
        key = (method, route, status)
        with self._lock:
            count, duration = self._values.get(key, (0, 0.0))
            self._values[key] = (count + 1, duration + max(0.0, duration_seconds))

    def render(self) -> bytes:
        with self._lock:
            values = sorted(self._values.items())
        lines = [
            "# HELP merchant_http_requests_total HTTP requests completed by bounded route.",
            "# TYPE merchant_http_requests_total counter",
        ]
        for (method, route, status), (count, _) in values:
            labels = f'method="{method}",route="{route}",status="{status}"'
            lines.append(f"merchant_http_requests_total{{{labels}}} {count}")
        lines.extend(
            [
                "# HELP merchant_http_request_duration_seconds Request duration by bounded route.",
                "# TYPE merchant_http_request_duration_seconds summary",
            ]
        )
        for (method, route, status), (count, _) in values:
            labels = f'method="{method}",route="{route}",status="{status}"'
            lines.append(f"merchant_http_request_duration_seconds_count{{{labels}}} {count}")
        for (method, route, status), (_, duration) in values:
            labels = f'method="{method}",route="{route}",status="{status}"'
            lines.append(f"merchant_http_request_duration_seconds_sum{{{labels}}} {duration:.6f}")
        return ("\n".join(lines) + "\n").encode()


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
        trace_observer: Callable[[Mapping[str, object]], None] | None = None,
        request_id_factory: Callable[[], str] | None = None,
        trace_id_factory: Callable[[], str] | None = None,
        span_id_factory: Callable[[], str] | None = None,
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
        self._trace_observer = trace_observer
        self._request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)
        self._trace_id_factory = trace_id_factory or (lambda: uuid.uuid4().hex)
        self._span_id_factory = span_id_factory or (lambda: uuid.uuid4().hex[:16])
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._request_metrics = _RequestMetrics()
        self._shutdown_started = threading.Event()

    def begin_shutdown(self) -> bool:
        """Remove the process from readiness before the server stops accepting work."""

        if self._shutdown_started.is_set():
            return False
        self._shutdown_started.set()
        return True

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
        incoming_trace = _parse_traceparent(environ.get("HTTP_TRACEPARENT"))
        trace_id = (
            incoming_trace[0]
            if incoming_trace is not None
            else _validated_hex_id(self._trace_id_factory(), 32, "trace_id_factory")
        )
        parent_span_id = incoming_trace[1] if incoming_trace is not None else None
        trace_flags = incoming_trace[2] if incoming_trace is not None else "01"
        span_id = _validated_hex_id(self._span_id_factory(), 16, "span_id_factory")
        response_traceparent = f"00-{trace_id}-{span_id}-{trace_flags}"

        def observed_start_response(status: str, headers: list[tuple[str, str]]) -> Any:
            nonlocal response_status
            response_status = int(status.partition(" ")[0])
            return start_response(
                status,
                [*headers, ("X-Request-ID", request_id), ("traceparent", response_traceparent)],
            )

        response = self._dispatch(environ, observed_start_response)
        duration_seconds = max(0.0, self._monotonic_clock() - started)
        method = environ.get("REQUEST_METHOD", "").upper()
        path = environ.get("PATH_INFO", "")
        route = (
            path
            if path in {"/events", "/inventory", "/healthz", "/readyz", "/metrics"}
            else "unknown"
        )
        metric_method = method if method in {"GET", "POST"} else "OTHER"
        self._request_metrics.observe(metric_method, route, response_status, duration_seconds)
        if self._trace_observer is not None:
            self._trace_observer(
                {
                    "duration_ms": round(duration_seconds * 1000, 3),
                    "method": metric_method,
                    "parent_span_id": parent_span_id,
                    "route": route,
                    "span_id": span_id,
                    "status": response_status,
                    "trace_id": trace_id,
                }
            )
        if self._access_log is not None:
            self._access_log(
                {
                    "duration_ms": round(duration_seconds * 1000, 3),
                    "method": method,
                    "path": path,
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
        if path == "/readyz":
            if method != "GET":
                return self._respond(start_response, "405 Method Not Allowed", {"error": "method not allowed"}, [("Allow", "GET")])
            if self._shutdown_started.is_set() or not self.store.is_ready():
                return self._respond(
                    start_response,
                    "503 Service Unavailable",
                    {"status": "unavailable"},
                )
            return self._respond(start_response, "200 OK", {"status": "ready"})
        if path == "/metrics":
            if method != "GET":
                return self._respond(start_response, "405 Method Not Allowed", {"error": "method not allowed"}, [("Allow", "GET")])
            return self._metrics(start_response)
        return self._respond(start_response, "404 Not Found", {"error": "not found"})

    def _metrics(self, start_response: StartResponse) -> list[bytes]:
        body = self._request_metrics.render()
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/plain; version=0.0.4; charset=utf-8"),
                ("Content-Length", str(len(body))),
                ("Cache-Control", "no-store"),
            ],
        )
        return [body]

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


def _write_trace_span(event: Mapping[str, object]) -> None:
    """Write a bounded demo span suitable for replacing with a real exporter."""

    print(json.dumps(event, separators=(",", ":"), sort_keys=True), file=sys.stderr, flush=True)


def _shutdown_handler(app: InventoryHTTPApp, server: Any) -> Callable[[int, Any], None]:
    """Build a signal handler that drains the single-threaded demo without deadlock."""

    def handle_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        if not app.begin_shutdown():
            return
        # BaseServer.shutdown must run from a different thread than serve_forever.
        threading.Thread(
            target=server.shutdown,
            name="merchant-event-shutdown",
            daemon=False,
        ).start()

    return handle_shutdown


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
            trace_observer=_write_trace_span,
        )
    except (json.JSONDecodeError, UnicodeEncodeError, ValueError):
        parser.error("MERCHANT_EVENT_SECRETS must map key IDs to string secrets")
    with make_server(args.host, args.port, app) as server:
        shutdown_handler = _shutdown_handler(app, server)
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        print(f"Serving local demo on http://{args.host}:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
