# Merchant Event Lab

An independent, synthetic merchant-inventory engineering portfolio project. Not affiliated with Shopify. No merchant data or Shopify credentials are used.

## First slice: durable, idempotent inventory events

A SQLite-backed event ledger accepts `(event_id, sku, delta)` changes in integer stock units. An exact replay has no effect; an ID reused with a different payload fails. Stock cannot go below zero. Changes and validation occur within a write transaction, including across separate processes/connections. A rejected sale is not recorded and can be retried after restocking.

```python
from merchant_event_lab.inventory import InventoryStore

store = InventoryStore("inventory.sqlite3")
store.apply("restock-001", "demo-sku", 5)  # True
store.apply("restock-001", "demo-sku", 5)  # False: replay
store.apply("sale-001", "demo-sku", -2)     # True
assert store.stock("demo-sku") == 3
```

Run tests with `python3 -m unittest discover -s tests -v`. Python standard library only.

This is a local event-core prototype, not yet a webhook server or a production inventory system. Future slices can add an authenticated ingestion boundary, operational dashboard, and failure/concurrency tests. SQLite integer sums have finite range; extremely large values are not yet bounded at the application layer.
