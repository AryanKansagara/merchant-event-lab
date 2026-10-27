"""HTTP-agnostic authenticated boundary for synthetic inventory events."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .inventory import InventoryStore


_KEY_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")


class AuthenticationError(ValueError):
    """Raised when a payload does not have a valid, current signature."""


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
    """Verify a timestamped HMAC-SHA256 signature and apply one event.

    The signature covers ``<timestamp>.<exact request bytes>`` and must use the
    form ``sha256=<64 hexadecimal characters>``. A key identifier selects one
    configured secret, allowing an old and new key to overlap during rotation.
    Transport adapters must pass the unmodified request body and header values.
    """

    def __init__(
        self,
        store: InventoryStore,
        secrets: Mapping[str, bytes],
        *,
        max_payload_bytes: int = 64 * 1024,
        max_age_seconds: int = 5 * 60,
        clock: Callable[[], float] = time.time,
    ):
        if not isinstance(secrets, Mapping) or not secrets:
            raise ValueError("secrets must be a non-empty mapping")
        validated_secrets: dict[str, bytes] = {}
        for key_id, secret in secrets.items():
            if not isinstance(key_id, str) or not _KEY_ID_PATTERN.fullmatch(key_id):
                raise ValueError("key IDs must be 1-64 URL-safe characters")
            if not isinstance(secret, bytes) or not secret:
                raise ValueError("secrets must be non-empty bytes")
            validated_secrets[key_id] = secret
        if type(max_payload_bytes) is not int or max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be a positive integer")
        if type(max_age_seconds) is not int or max_age_seconds < 0:
            raise ValueError("max_age_seconds must be a non-negative integer")
        if not callable(clock):
            raise ValueError("clock must be callable")

        self.store = store
        self._secrets = validated_secrets
        self.max_payload_bytes = max_payload_bytes
        self.max_age_seconds = max_age_seconds
        self._clock = clock

    def ingest(
        self,
        body: bytes,
        signature: str,
        key_id: str,
        timestamp: str,
    ) -> bool:
        """Authenticate and apply an event; return False for an exact replay."""
        if not isinstance(body, bytes):
            raise TypeError("body must be bytes")
        if len(body) > self.max_payload_bytes:
            raise PayloadError("payload exceeds size limit")

        if not isinstance(timestamp, str) or not timestamp.isascii() or not timestamp.isdecimal():
            raise AuthenticationError("invalid authentication envelope")
        event_time = int(timestamp)
        if str(event_time) != timestamp:
            raise AuthenticationError("invalid authentication envelope")

        secret = self._secrets.get(key_id) if isinstance(key_id, str) else None
        if secret is None:
            raise AuthenticationError("invalid authentication envelope")
        signed_message = timestamp.encode("ascii") + b"." + body
        expected = "sha256=" + hmac.new(secret, signed_message, hashlib.sha256).hexdigest()
        if not isinstance(signature, str) or not hmac.compare_digest(
            signature.lower(), expected
        ):
            raise AuthenticationError("invalid authentication envelope")

        now = int(self._clock())
        if abs(now - event_time) > self.max_age_seconds:
            raise AuthenticationError("signature timestamp outside allowed window")

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
