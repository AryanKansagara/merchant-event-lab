import hashlib
import hmac
import tempfile
import unittest
from pathlib import Path

from merchant_event_lab.ingestion import (
    AuthenticationError,
    PayloadError,
    SignedEventIngestor,
)
from merchant_event_lab.inventory import InventoryStore


class SignedEventIngestorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = InventoryStore(Path(self.tmp.name) / "events.sqlite3")
        self.secret = b"test-secret"
        self.now = 1_800_000_000
        self.timestamp = str(self.now)
        self.ingestor = SignedEventIngestor(
            self.store,
            {"current": self.secret},
            clock=lambda: self.now,
        )

    def sign(self, body, *, secret=None, timestamp=None):
        selected_secret = self.secret if secret is None else secret
        selected_timestamp = self.timestamp if timestamp is None else timestamp
        message = selected_timestamp.encode() + b"." + body
        return "sha256=" + hmac.new(
            selected_secret, message, hashlib.sha256
        ).hexdigest()

    def ingest(self, body, signature=None, *, key_id="current", timestamp=None):
        return self.ingestor.ingest(
            body,
            self.sign(body) if signature is None else signature,
            key_id,
            self.timestamp if timestamp is None else timestamp,
        )

    def test_valid_signed_event_is_applied_and_replays_are_idempotent(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        signature = self.sign(body)

        self.assertTrue(self.ingest(body, signature))
        self.assertFalse(self.ingest(body, signature))
        self.assertEqual(self.store.stock("demo"), 4)

    def test_tampered_body_is_rejected_before_mutation(self):
        original = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        tampered = b'{"event_id":"restock-1","sku":"demo","delta":40}'

        with self.assertRaisesRegex(AuthenticationError, "invalid authentication"):
            self.ingest(tampered, self.sign(original))
        self.assertEqual(self.store.stock("demo"), 0)

    def test_malformed_signature_is_rejected(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        for signature in ("", "not-a-signature", "sha256=00"):
            with self.subTest(signature=signature):
                with self.assertRaises(AuthenticationError):
                    self.ingest(body, signature)

    def test_authenticated_payload_must_match_strict_contract(self):
        invalid_bodies = [
            b"[]",
            b'{"event_id":"one","sku":"demo"}',
            b'{"event_id":"one","sku":"demo","delta":1,"extra":true}',
            b'{"event_id":"one","event_id":"two","sku":"demo","delta":1}',
            b'{"event_id":"one","sku":"demo","delta":true}',
            b'{"event_id":"one","sku":"demo","delta":NaN}',
            b"\xff",
        ]
        for body in invalid_bodies:
            with self.subTest(body=body):
                with self.assertRaises(PayloadError):
                    self.ingest(body)
        self.assertEqual(self.store.stock("demo"), 0)

    def test_payload_size_is_checked_before_authentication(self):
        ingestor = SignedEventIngestor(
            self.store, {"current": self.secret}, max_payload_bytes=4
        )
        with self.assertRaisesRegex(PayloadError, "size limit"):
            ingestor.ingest(b"12345", "invalid", "current", self.timestamp)

    def test_inventory_failures_are_reported_as_payload_errors(self):
        body = b'{"event_id":"sale-1","sku":"demo","delta":-1}'
        with self.assertRaisesRegex(PayloadError, "insufficient stock"):
            self.ingest(body)

    def test_constructor_rejects_invalid_security_configuration(self):
        invalid_configurations = [
            ({}, {}),
            ({"": self.secret}, {}),
            ({"not allowed": self.secret}, {}),
            ({"current": b""}, {}),
            ({"current": self.secret}, {"max_payload_bytes": 0}),
            ({"current": self.secret}, {"max_age_seconds": -1}),
            ({"current": self.secret}, {"clock": None}),
        ]
        for secrets, options in invalid_configurations:
            with self.subTest(secrets=secrets, options=options):
                with self.assertRaises(ValueError):
                    SignedEventIngestor(self.store, secrets, **options)

    def test_rotation_window_accepts_each_configured_key(self):
        old_secret = b"old-test-secret"
        ingestor = SignedEventIngestor(
            self.store,
            {"old": old_secret, "current": self.secret},
            clock=lambda: self.now,
        )
        old_body = b'{"event_id":"old-key","sku":"demo","delta":2}'
        new_body = b'{"event_id":"new-key","sku":"demo","delta":3}'

        self.assertTrue(
            ingestor.ingest(
                old_body,
                self.sign(old_body, secret=old_secret),
                "old",
                self.timestamp,
            )
        )
        self.assertTrue(
            ingestor.ingest(new_body, self.sign(new_body), "current", self.timestamp)
        )
        self.assertEqual(self.store.stock("demo"), 5)

    def test_unknown_key_and_cross_key_signature_are_rejected(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        for key_id in ("missing", "current"):
            signature = self.sign(body, secret=b"wrong")
            with self.subTest(key_id=key_id):
                with self.assertRaisesRegex(
                    AuthenticationError, "invalid authentication envelope"
                ):
                    self.ingest(body, signature, key_id=key_id)
        self.assertEqual(self.store.stock("demo"), 0)

    def test_stale_and_far_future_timestamps_are_rejected(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        for timestamp in (str(self.now - 301), str(self.now + 301)):
            with self.subTest(timestamp=timestamp):
                with self.assertRaisesRegex(AuthenticationError, "allowed window"):
                    self.ingest(
                        body,
                        self.sign(body, timestamp=timestamp),
                        timestamp=timestamp,
                    )
        self.assertEqual(self.store.stock("demo"), 0)

    def test_timestamp_is_canonical_and_covered_by_signature(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        for timestamp in ("", "+timestamp", "01800000000", "not-a-time"):
            with self.subTest(timestamp=timestamp):
                with self.assertRaises(AuthenticationError):
                    self.ingest(body, self.sign(body), timestamp=timestamp)

        with self.assertRaises(AuthenticationError):
            self.ingest(body, self.sign(body), timestamp=str(self.now - 1))
        self.assertEqual(self.store.stock("demo"), 0)


if __name__ == "__main__":
    unittest.main()
