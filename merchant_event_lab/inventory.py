"""Persistent, idempotent inventory event ledger (integer units only)."""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path


MAX_STOCK = 2**63 - 1


@dataclass(frozen=True)
class InventoryPosition:
    """Current stock for one SKU."""

    sku: str
    stock: int


@dataclass(frozen=True)
class InventoryPage:
    """A deterministic keyset-paginated inventory page."""

    items: tuple[InventoryPosition, ...]
    next_cursor: str | None


class InventoryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        with closing(sqlite3.connect(self.path)) as db, db:
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
        if (
            type(delta) is not int
            or delta == 0
            or not -(2**63) <= delta <= MAX_STOCK
        ):
            raise ValueError("delta must be a non-zero signed 64-bit integer")

        with closing(sqlite3.connect(self.path, timeout=10)) as db, db:
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
            if current + delta > MAX_STOCK:
                raise ValueError("stock limit exceeded")
            db.execute(
                "INSERT INTO events (event_id, sku, delta) VALUES (?, ?, ?)",
                (event_id, sku, delta),
            )
            return True

    def stock(self, sku: str) -> int:
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute(
                "SELECT COALESCE(SUM(delta), 0) FROM events WHERE sku = ?", (sku,)
            ).fetchone()[0]

    def is_ready(self) -> bool:
        """Return whether the existing database and event schema are readable.

        Read-write URI mode prevents a missing database from being silently
        created by a readiness check. Database details are deliberately reduced
        to a boolean so transport adapters do not expose paths or SQL errors.
        """

        database_uri = f"{self.path.resolve().as_uri()}?mode=rw"
        try:
            with closing(sqlite3.connect(database_uri, uri=True, timeout=1)) as db:
                db.execute("SELECT event_id, sku, delta FROM events LIMIT 1").fetchone()
        except (OSError, sqlite3.Error):
            return False
        return True

    def inventory_page(
        self,
        *,
        limit: int = 100,
        after_sku: str | None = None,
        low_stock_at: int | None = None,
    ) -> InventoryPage:
        """Return current positions ordered by SKU using keyset pagination.

        ``after_sku`` is the ``next_cursor`` from a previous page. When
        ``low_stock_at`` is provided, only positions at or below that stock
        level are returned. SKUs with zero stock remain visible because they
        have event history and may need replenishment.
        """
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000")
        if after_sku is not None and (
            not isinstance(after_sku, str) or not after_sku.strip()
        ):
            raise ValueError("after_sku must be a non-empty string")
        if low_stock_at is not None and (
            type(low_stock_at) is not int or not 0 <= low_stock_at <= MAX_STOCK
        ):
            raise ValueError("low_stock_at must be a non-negative 64-bit integer")

        filters: list[str] = []
        parameters: list[str | int] = []
        if after_sku is not None:
            filters.append("sku > ?")
            parameters.append(after_sku)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""

        having = ""
        if low_stock_at is not None:
            having = "HAVING SUM(delta) <= ?"
            parameters.append(low_stock_at)
        parameters.append(limit + 1)

        with closing(sqlite3.connect(self.path)) as db, db:
            rows = db.execute(
                f"""SELECT sku, SUM(delta) AS stock
                    FROM events
                    {where}
                    GROUP BY sku
                    {having}
                    ORDER BY sku
                    LIMIT ?""",
                parameters,
            ).fetchall()

        has_more = len(rows) > limit
        visible_rows = rows[:limit]
        items = tuple(InventoryPosition(sku, stock) for sku, stock in visible_rows)
        return InventoryPage(
            items=items,
            next_cursor=items[-1].sku if has_more else None,
        )
