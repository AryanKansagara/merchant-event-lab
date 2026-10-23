"""Persistent, idempotent inventory event ledger (integer units only)."""

import sqlite3
from pathlib import Path


class InventoryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        with sqlite3.connect(self.path) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                delta INTEGER NOT NULL CHECK(delta != 0)
            )""")

    def apply(self, event_id: str, sku: str, delta: int) -> bool:
        """Apply a stock change; return False for an exact replay.

        An event ID reused with different data is an error. Rejected sales are not
        recorded, so they may be retried after restocking.
        """
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id must be a non-empty string")
        if not isinstance(sku, str) or not sku.strip():
            raise ValueError("sku must be a non-empty string")
        if type(delta) is not int or delta == 0:
            raise ValueError("delta must be a non-zero integer")

        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT sku, delta FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if prior is not None:
                if prior != (sku, delta):
                    raise ValueError("event ID conflict")
                return False
            current = db.execute(
                "SELECT COALESCE(SUM(delta), 0) FROM events WHERE sku = ?", (sku,)
            ).fetchone()[0]
            if current + delta < 0:
                raise ValueError("insufficient stock")
            db.execute(
                "INSERT INTO events (event_id, sku, delta) VALUES (?, ?, ?)",
                (event_id, sku, delta),
            )
            return True

    def stock(self, sku: str) -> int:
        with sqlite3.connect(self.path) as db:
            return db.execute(
                "SELECT COALESCE(SUM(delta), 0) FROM events WHERE sku = ?", (sku,)
            ).fetchone()[0]
