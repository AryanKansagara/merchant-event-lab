"""HTTP-agnostic authenticated boundary for synthetic inventory events."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Iterable
from typing import Any

from .inventory import InventoryStore


class AuthenticationError(ValueError):
    """Raised when a payload does not have a valid signature."""


class PayloadError(ValueError):
    """Raised when an authenticated payload does not match the event contract."""


def _object_without_duplicate_keys(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PayloadError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


class SignedEventIngestor:
    """Verify an HMAC-SHA256 signature, parse one event, and apply it.

    The signature covers the exact request bytes and must use the form
    ``sha256=<64 hexadecimal characters>``. Transport adapters should pass the
    unmodified request body rather than decoded or re-serialized JSON.
    """

    def __init__(
        self,
        store: InventoryStore,
        secret: bytes,
        *,
        max_payload_bytes: int = 64 * 1024,
    ):
        if not isinstance(secret, bytes) or not secret:
            raise ValueError("secret must be non-empty bytes")
        if type(max_payload_bytes) is not int or max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be a positive integer")
        self.store = store
        self._secret = secret
        self.max_payload_bytes = max_payload_bytes

    def ingest(self, body: bytes, signature: str) -> bool:
        """Authenticate and apply an event; return False for an exact replay."""
        if not isinstance(body, bytes):
            raise TypeError("body must be bytes")
        if len(body) > self.max_payload_bytes:
            raise PayloadError("payload exceeds size limit")

        expected = "sha256=" + hmac.new(
            self._secret, body, hashlib.sha256
        ).hexdigest()
        if not isinstance(signature, str) or not hmac.compare_digest(
            signature.lower(), expected
        ):
            raise AuthenticationError("invalid signature")

        try:
            payload = json.loads(
                body,
                object_pairs_hook=_object_without_duplicate_keys,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    PayloadError(f"invalid JSON constant: {value}")
                ),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PayloadError("body must be valid UTF-8 JSON") from error

        if not isinstance(payload, dict) or set(payload) != {
            "event_id",
            "sku",
            "delta",
        }:
            raise PayloadError("payload must contain only event_id, sku, and delta")

        try:
            return self.store.apply(
                payload["event_id"], payload["sku"], payload["delta"]
            )
        except ValueError as error:
            raise PayloadError(str(error)) from error
