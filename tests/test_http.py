import hashlib
import hmac
import io
import json
import tempfile
import unittest
from pathlib import Path

from merchant_event_lab.http import InventoryHTTPApp
from merchant_event_lab.inventory import InventoryStore


class InventoryHTTPAppTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = InventoryStore(Path(self.tmp.name) / "events.sqlite3")
        self.secret = b"http-test-secret"
        self.now = 1_800_000_000
        self.timestamp = str(self.now)
        self.app = InventoryHTTPApp(
            self.store,
            {"current": self.secret},
            max_payload_bytes=128,
            clock=lambda: self.now,
        )

    def request(
        self,
        method,
        path,
        *,
        body=b"",
        query="",
        content_type="",
        signature="",
        key_id="current",
        timestamp=None,
        content_length=None,
    ):
        captured = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        environ = {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "QUERY_STRING": query,
            "CONTENT_TYPE": content_type,
            "CONTENT_LENGTH": str(len(body)) if content_length is None else content_length,
            "HTTP_X_EVENT_SIGNATURE": signature,
            "HTTP_X_EVENT_KEY_ID": key_id,
            "HTTP_X_EVENT_TIMESTAMP": self.timestamp if timestamp is None else timestamp,
            "wsgi.input": io.BytesIO(body),
        }
        response_body = b"".join(self.app(environ, start_response))
        self.assertEqual(int(captured["headers"]["Content-Length"]), len(response_body))
        self.assertEqual(captured["headers"]["Cache-Control"], "no-store")
        return captured["status"], captured["headers"], json.loads(response_body)

    def sign(self, body, *, timestamp=None):
        selected_timestamp = self.timestamp if timestamp is None else timestamp
        message = selected_timestamp.encode() + b"." + body
        return "sha256=" + hmac.new(self.secret, message, hashlib.sha256).hexdigest()

    def post_event(self, body):
        return self.request(
            "POST",
            "/events",
            body=body,
            content_type="application/json",
            signature=self.sign(body),
        )

    def test_signed_event_is_applied_and_replay_is_reported(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'

        status, _, payload = self.post_event(body)
        self.assertEqual((status, payload), ("202 Accepted", {"status": "applied"}))

        status, _, payload = self.post_event(body)
        self.assertEqual((status, payload), ("200 OK", {"status": "replay"}))
        self.assertEqual(self.store.stock("demo"), 4)

    def test_event_endpoint_maps_client_failures_without_mutation(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        cases = [
            ({"content_type": "text/plain", "signature": self.sign(body)}, "415 Unsupported Media Type"),
            ({"content_type": "application/json", "signature": "bad"}, "401 Unauthorized"),
            ({"content_type": "application/json", "signature": self.sign(body), "content_length": "bad"}, "411 Length Required"),
            ({"content_type": "application/json", "signature": self.sign(body), "content_length": "129"}, "413 Payload Too Large"),
        ]
        for options, expected_status in cases:
            with self.subTest(status=expected_status):
                status, _, payload = self.request("POST", "/events", body=body, **options)
                self.assertEqual(status, expected_status)
                self.assertIn("error", payload)
        self.assertEqual(self.store.stock("demo"), 0)

    def test_event_endpoint_rejects_missing_key_id_and_stale_timestamp(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'

        status, _, _ = self.request(
            "POST",
            "/events",
            body=body,
            content_type="application/json",
            signature=self.sign(body),
            key_id="",
        )
        self.assertEqual(status, "401 Unauthorized")

        stale = str(self.now - 301)
        status, _, _ = self.request(
            "POST",
            "/events",
            body=body,
            content_type="application/json",
            signature=self.sign(body, timestamp=stale),
            timestamp=stale,
        )
        self.assertEqual(status, "401 Unauthorized")
        self.assertEqual(self.store.stock("demo"), 0)

    def test_event_endpoint_rejects_short_or_invalid_authenticated_body(self):
        valid = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        status, _, _ = self.request(
            "POST",
            "/events",
            body=valid,
            content_type="application/json",
            signature=self.sign(valid),
            content_length=str(len(valid) + 1),
        )
        self.assertEqual(status, "400 Bad Request")

        invalid = b"{}"
        status, _, payload = self.post_event(invalid)
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("payload must contain", payload["error"])

    def test_event_endpoint_rate_limit_recovers_after_window(self):
        self.app = InventoryHTTPApp(
            self.store,
            {"current": self.secret},
            event_rate_limit=2,
            rate_window_seconds=30,
            clock=lambda: self.now,
        )
        first = b'{"event_id":"restock-1","sku":"demo","delta":1}'
        second = b'{"event_id":"restock-2","sku":"demo","delta":1}'
        third = b'{"event_id":"restock-3","sku":"demo","delta":1}'

        self.assertEqual(self.post_event(first)[0], "202 Accepted")
        self.assertEqual(self.post_event(second)[0], "202 Accepted")
        status, headers, payload = self.post_event(third)
        self.assertEqual(status, "429 Too Many Requests")
        self.assertEqual(headers["Retry-After"], "30")
        self.assertIn("rate limit", payload["error"])
        self.assertEqual(self.store.stock("demo"), 2)

        self.now += 30
        status, _, _ = self.post_event(third)
        self.assertEqual(status, "202 Accepted")
        self.assertEqual(self.store.stock("demo"), 3)

    def test_event_rate_limit_configuration_must_be_positive(self):
        with self.assertRaises(ValueError):
            InventoryHTTPApp(
                self.store,
                {"current": self.secret},
                event_rate_limit=0,
            )
        with self.assertRaises(ValueError):
            InventoryHTTPApp(
                self.store,
                {"current": self.secret},
                rate_window_seconds=0,
            )

    def test_inventory_endpoint_returns_filtered_keyset_pages(self):
        self.store.apply("a", "alpha", 1)
        self.store.apply("b", "beta", 5)
        self.store.apply("c", "gamma", 2)

        status, _, payload = self.request("GET", "/inventory", query="limit=1&low_stock_at=2")
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload, {"items": [{"sku": "alpha", "stock": 1}], "next_cursor": "alpha"})

        status, _, payload = self.request("GET", "/inventory", query="limit=2&low_stock_at=2&after_sku=alpha")
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload, {"items": [{"sku": "gamma", "stock": 2}], "next_cursor": None})

    def test_inventory_endpoint_rejects_ambiguous_or_invalid_queries(self):
        for query in ("unknown=1", "limit=1&limit=2", "limit=nope", "after_sku="):
            with self.subTest(query=query):
                status, _, payload = self.request("GET", "/inventory", query=query)
                self.assertEqual(status, "400 Bad Request")
                self.assertIn("error", payload)

    def test_routing_health_and_method_contracts(self):
        status, _, payload = self.request("GET", "/healthz")
        self.assertEqual((status, payload), ("200 OK", {"status": "ok"}))

        status, headers, _ = self.request("GET", "/events")
        self.assertEqual(status, "405 Method Not Allowed")
        self.assertEqual(headers["Allow"], "POST")

        status, _, _ = self.request("GET", "/missing")
        self.assertEqual(status, "404 Not Found")

    def test_structured_access_observability_correlates_response(self):
        records = []
        monotonic_values = iter((10.0, 10.012345))
        self.app = InventoryHTTPApp(
            self.store,
            {"current": self.secret},
            access_log=records.append,
            request_id_factory=lambda: "request-123",
            monotonic_clock=lambda: next(monotonic_values),
            clock=lambda: self.now,
        )

        status, headers, _ = self.request("GET", "/inventory", query="low_stock_at=2")

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["X-Request-ID"], "request-123")
        self.assertEqual(
            records,
            [
                {
                    "duration_ms": 12.345,
                    "method": "GET",
                    "path": "/inventory",
                    "request_id": "request-123",
                    "status": 200,
                }
            ],
        )
        self.assertNotIn("low_stock_at", json.dumps(records))

    def test_access_observability_records_rejected_requests_without_sensitive_data(self):
        records = []
        self.app = InventoryHTTPApp(
            self.store,
            {"current": self.secret},
            access_log=records.append,
            request_id_factory=lambda: "rejected-123",
            monotonic_clock=lambda: 1.0,
            clock=lambda: self.now,
        )
        secret_body = b'{"secret":"must-not-be-logged"}'

        status, headers, _ = self.request(
            "POST",
            "/events",
            body=secret_body,
            content_type="text/plain",
            signature="must-not-be-logged",
        )

        self.assertEqual(status, "415 Unsupported Media Type")
        self.assertEqual(headers["X-Request-ID"], "rejected-123")
        self.assertEqual(records[0]["status"], 415)
        serialized = json.dumps(records)
        self.assertNotIn("secret", serialized)
        self.assertNotIn("signature", serialized)

    def test_request_id_factory_cannot_inject_response_headers(self):
        self.app = InventoryHTTPApp(
            self.store,
            {"current": self.secret},
            request_id_factory=lambda: "bad\r\nInjected: yes",
            clock=lambda: self.now,
        )

        with self.assertRaisesRegex(ValueError, "printable ASCII"):
            self.request("GET", "/healthz")


if __name__ == "__main__":
    unittest.main()
