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
        self.ingestor = SignedEventIngestor(self.store, self.secret)

    def sign(self, body):
        return "sha256=" + hmac.new(self.secret, body, hashlib.sha256).hexdigest()

    def test_valid_signed_event_is_applied_and_replays_are_idempotent(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        signature = self.sign(body)

        self.assertTrue(self.ingestor.ingest(body, signature))
        self.assertFalse(self.ingestor.ingest(body, signature))
        self.assertEqual(self.store.stock("demo"), 4)

    def test_tampered_body_is_rejected_before_mutation(self):
        original = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        tampered = b'{"event_id":"restock-1","sku":"demo","delta":40}'

        with self.assertRaisesRegex(AuthenticationError, "invalid signature"):
            self.ingestor.ingest(tampered, self.sign(original))
        self.assertEqual(self.store.stock("demo"), 0)

    def test_malformed_signature_is_rejected(self):
        body = b'{"event_id":"restock-1","sku":"demo","delta":4}'
        for signature in ("", "not-a-signature", "sha256=00"):
            with self.subTest(signature=signature):
                with self.assertRaises(AuthenticationError):
                    self.ingestor.ingest(body, signature)

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
                    self.ingestor.ingest(body, self.sign(body))
        self.assertEqual(self.store.stock("demo"), 0)

    def test_payload_size_is_checked_before_authentication(self):
        ingestor = SignedEventIngestor(self.store, self.secret, max_payload_bytes=4)
        with self.assertRaisesRegex(PayloadError, "size limit"):
            ingestor.ingest(b"12345", "invalid")

    def test_inventory_failures_are_reported_as_payload_errors(self):
        body = b'{"event_id":"sale-1","sku":"demo","delta":-1}'
        with self.assertRaisesRegex(PayloadError, "insufficient stock"):
            self.ingestor.ingest(body, self.sign(body))

    def test_constructor_rejects_invalid_security_configuration(self):
        with self.assertRaises(ValueError):
            SignedEventIngestor(self.store, b"")
        with self.assertRaises(ValueError):
            SignedEventIngestor(self.store, self.secret, max_payload_bytes=0)


if __name__ == "__main__":
    unittest.main()
