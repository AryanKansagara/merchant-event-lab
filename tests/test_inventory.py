import tempfile
import unittest
from pathlib import Path

from merchant_event_lab.inventory import InventoryStore


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
        for event_id, sku, delta in [("", "sku-a", 1), ("evt-1", "", 1), ("evt-1", "sku-a", 0), ("evt-1", "sku-a", True)]:
            with self.subTest(event_id=event_id, sku=sku, delta=delta):
                with self.assertRaises(ValueError):
                    self.store.apply(event_id, sku, delta)
        self.assertEqual(self.store.stock("sku-a"), 0)


if __name__ == "__main__":
    unittest.main()
