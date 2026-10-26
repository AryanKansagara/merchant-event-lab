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

An operational read model provides deterministic keyset pagination over current
SKU positions. It can also select low-stock and zero-stock positions for a
replenishment view without exposing raw event rows:

```python
page = store.inventory_page(limit=100, low_stock_at=5)
for position in page.items:
    print(position.sku, position.stock)

if page.next_cursor is not None:
    next_page = store.inventory_page(limit=100, after_sku=page.next_cursor)
```

A page is read-consistent, but separate pages are not a database snapshot when
writes happen between requests.

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

The signature header contract is `sha256=<hex digest>`. Transport adapters must
retain the raw bytes and use TLS. Exact signed replays are safe because the event
store is idempotent.

## Local HTTP demo

A standard-library WSGI adapter exposes the complete flow without external
services:

- `POST /events` requires `Content-Type: application/json`, `Content-Length`, and
  `X-Event-Signature`; it reports new events as `202` and exact replays as `200`.
- `GET /inventory?limit=100&after_sku=...&low_stock_at=...` returns the
  operational read model as deterministic JSON.
- `GET /healthz` provides a process-level health response.

Run the local-only demo server with a secret supplied through the environment:

```bash
MERCHANT_EVENT_SECRET='local-demo-only' \
  python3 -m merchant_event_lab.http --database inventory.sqlite3 --port 8000
```

For example, sign the exact body bytes and submit one synthetic restock:

```bash
body='{"event_id":"restock-001","sku":"demo-sku","delta":5}'
signature="sha256=$(printf %s "$body" | openssl dgst -sha256 -hmac "$MERCHANT_EVENT_SECRET" -hex | cut -d' ' -f2)"
curl -i http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -H "X-Event-Signature: $signature" \
  --data-binary "$body"
curl -s 'http://127.0.0.1:8000/inventory?low_stock_at=5'
```

The bundled `wsgiref` server is for a local demonstration, not production. The
prototype does not provide TLS termination, time-based replay expiry, secret
rotation, distributed rate limiting, or production observability.
