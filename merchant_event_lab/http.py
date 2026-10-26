"""Small WSGI adapter for the synthetic merchant event lab."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Iterable
from typing import Any
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from .ingestion import AuthenticationError, PayloadError, SignedEventIngestor
from .inventory import InventoryStore

StartResponse = Callable[[str, list[tuple[str, str]]], Any]


class InventoryHTTPApp:
    """Expose signed event ingestion and inventory reads through WSGI.

    This adapter intentionally uses only the Python standard library. It is a
    local demonstration boundary, not a production web server.
    """

    def __init__(self, store: InventoryStore, secret: bytes, *, max_payload_bytes: int = 64 * 1024):
        self.store = store
        self.ingestor = SignedEventIngestor(
            store, secret, max_payload_bytes=max_payload_bytes
        )

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
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
            applied = self.ingestor.ingest(body, environ.get("HTTP_X_EVENT_SIGNATURE", ""))
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local merchant-event demo API")
    parser.add_argument("--database", default="inventory.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    secret = os.environ.get("MERCHANT_EVENT_SECRET")
    if not secret:
        parser.error("MERCHANT_EVENT_SECRET must be set")
    app = InventoryHTTPApp(InventoryStore(args.database), secret.encode())
    with make_server(args.host, args.port, app) as server:
        print(f"Serving local demo on http://{args.host}:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
