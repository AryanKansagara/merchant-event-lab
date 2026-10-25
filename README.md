# Merchant Event Lab

An independent, synthetic merchant-inventory engineering portfolio project. Not affiliated with Shopify. No merchant data or Shopify credentials are used.

## Durable inventory events and authenticated ingestion

A SQLite-backed event ledger accepts `(event_id, sku, delta)` changes in integer stock units. An exact replay has no effect; an ID reused with a different payload fails. Stock is bounded to SQLite's signed 64-bit integer range and cannot go below zero. Changes and validation occur within a write transaction, including across separate processes/connections. A rejected sale is not recorded and can be retried after restocking.

```python
from merchant_event_lab.inventory import InventoryStore

store = InventoryStore("inventory.sqlite3")
store.apply("restock-001", "demo-sku", 5)  # True
store.apply("restock-001", "demo-sku", 5)  # False: replay
store.apply("sale-001", "demo-sku", -2)     # True
assert store.stock("demo-sku") == 3
```

Run tests with `python3 -m unittest discover -s tests -v`. Python standard library only.

`SignedEventIngestor` provides an HTTP-framework-agnostic boundary for synthetic
webhook-style events. It verifies an HMAC-SHA256 signature over the exact raw
request bytes before parsing JSON, enforces a payload-size limit, rejects
duplicate keys and non-standard numeric constants, and accepts only
`event_id`, `sku`, and `delta` fields.

```python
from merchant_event_lab.ingestion import SignedEventIngestor

ingestor = SignedEventIngestor(store, b"load-this-from-a-secret-manager")
applied = ingestor.ingest(raw_request_body, signature_header)
```

The signature header contract is `sha256=<hex digest>`. Real transport adapters
must retain the raw bytes and use TLS. Exact signed replays are safe because the
event store is idempotent; this prototype does not provide time-based replay
expiry or secret rotation. It is not yet a webhook server or production
inventory system. Future slices can add an HTTP adapter, secret rotation,
operational read model, and broader failure/concurrency tests.
