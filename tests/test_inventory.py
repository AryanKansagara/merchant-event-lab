import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from merchant_event_lab.inventory import (
    MAX_STOCK,
    InventoryPosition,
    InventoryStore,
)


class InventoryStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "events.sqlite3"
        self.store = InventoryStore(self.db)

    def test_repeated_event_is_applied_once_across_restarts(self):
        self.assertTrue(self.store.apply("evt-1", "sku-a", 7))
        self.assertFalse(InventoryStore(self.db).apply("evt-1", "sku-a", 7))
        self.assertEqual(self.store.stock("sku-a"), 7)

    def test_reused_event_id_with_different_payload_is_rejected(self):
        self.store.apply("evt-1", "sku-a", 7)
        with self.assertRaisesRegex(ValueError, "event ID conflict"):
            self.store.apply("evt-1", "sku-b", 7)
        self.assertEqual(self.store.stock("sku-b"), 0)

    def test_insufficient_stock_rolls_back_event(self):
        with self.assertRaisesRegex(ValueError, "insufficient stock"):
            self.store.apply("sale-1", "sku-a", -1)
        self.assertTrue(self.store.apply("restock-1", "sku-a", 2))
        self.assertTrue(self.store.apply("sale-1", "sku-a", -1))
        self.assertEqual(self.store.stock("sku-a"), 1)

    def test_invalid_inputs_do_not_create_events(self):
        invalid = [
            ("", "sku-a", 1),
            ("evt-1", "", 1),
            ("evt-1", "sku-a", 0),
            ("evt-1", "sku-a", True),
            ("evt-1", "sku-a", MAX_STOCK + 1),
            ("evt-1", "sku-a", -(2**63) - 1),
        ]
        for event_id, sku, delta in invalid:
            with self.subTest(event_id=event_id, sku=sku, delta=delta):
                with self.assertRaises(ValueError):
                    self.store.apply(event_id, sku, delta)
        self.assertEqual(self.store.stock("sku-a"), 0)

    def test_stock_overflow_rolls_back_event(self):
        self.store.apply("restock-max", "sku-a", MAX_STOCK)
        with self.assertRaisesRegex(ValueError, "stock limit exceeded"):
            self.store.apply("overflow", "sku-a", 1)

        self.assertEqual(self.store.stock("sku-a"), MAX_STOCK)
        self.assertTrue(self.store.apply("overflow", "sku-b", 1))

    def test_concurrent_writers_do_not_lose_updates(self):
        def apply(index):
            return InventoryStore(self.db).apply(f"evt-{index}", "sku-a", 1)

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(apply, range(16)))

        self.assertEqual(results, [True] * 16)
        self.assertEqual(self.store.stock("sku-a"), 16)

    def test_inventory_page_is_ordered_and_uses_keyset_cursor(self):
        for event_id, sku, delta in [
            ("restock-c", "sku-c", 3),
            ("restock-a", "sku-a", 1),
            ("restock-b", "sku-b", 2),
        ]:
            self.store.apply(event_id, sku, delta)

        first = self.store.inventory_page(limit=2)
        self.assertEqual(
            first.items,
            (
                InventoryPosition("sku-a", 1),
                InventoryPosition("sku-b", 2),
            ),
        )
        self.assertEqual(first.next_cursor, "sku-b")

        second = self.store.inventory_page(limit=2, after_sku=first.next_cursor)
        self.assertEqual(second.items, (InventoryPosition("sku-c", 3),))
        self.assertIsNone(second.next_cursor)

    def test_low_stock_page_includes_zero_stock_positions(self):
        self.store.apply("restock-a", "sku-a", 3)
        self.store.apply("sale-a", "sku-a", -3)
        self.store.apply("restock-b", "sku-b", 2)
        self.store.apply("restock-c", "sku-c", 4)

        first = self.store.inventory_page(limit=1, low_stock_at=2)
        self.assertEqual(first.items, (InventoryPosition("sku-a", 0),))
        self.assertEqual(first.next_cursor, "sku-a")

        second = self.store.inventory_page(
            limit=1,
            low_stock_at=2,
            after_sku=first.next_cursor,
        )
        self.assertEqual(second.items, (InventoryPosition("sku-b", 2),))
        self.assertIsNone(second.next_cursor)

    def test_inventory_page_rejects_invalid_options(self):
        invalid_options = [
            {"limit": 0},
            {"limit": 1001},
            {"limit": True},
            {"after_sku": ""},
            {"low_stock_at": -1},
            {"low_stock_at": True},
            {"low_stock_at": MAX_STOCK + 1},
        ]
        for options in invalid_options:
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    self.store.inventory_page(**options)


if __name__ == "__main__":
    unittest.main()
