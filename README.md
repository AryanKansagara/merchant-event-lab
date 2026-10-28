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
webhook-style events. It verifies an HMAC-SHA256 signature over a Unix timestamp
and the exact raw request bytes before parsing JSON, enforces a five-minute
freshness window and payload-size limit, rejects
duplicate keys and non-standard numeric constants, and accepts only
`event_id`, `sku`, and `delta` fields.

```python
from merchant_event_lab.ingestion import SignedEventIngestor

ingestor = SignedEventIngestor(
    store,
    {"2026-10": b"current-secret", "2026-09": b"retiring-secret"},
)
applied = ingestor.ingest(raw_body, signature_header, key_id, unix_timestamp)
```

The signature header contract is `sha256=<hex digest>`, calculated over
`<X-Event-Timestamp>.<raw body>`. `X-Event-Key-Id` selects a configured secret,
so a retiring key can overlap with its replacement. Remove the old key after
producers have migrated. Transport adapters must retain the raw bytes and use
TLS. The time window rejects delayed replays; exact in-window replays remain
safe because the event store is idempotent. This process-local key map is only a
demonstration—production deployments should use managed secret storage.

## Local HTTP demo

A standard-library WSGI adapter exposes the complete flow without external
services:

- `POST /events` requires `Content-Type: application/json`, `Content-Length`,
  `X-Event-Key-Id`, `X-Event-Timestamp`, and `X-Event-Signature`; it reports new
  events as `202` and exact replays as `200`. The demo applies a process-local
  fixed-window request limit and returns `429` with `Retry-After` when full.
- `GET /inventory?limit=100&after_sku=...&low_stock_at=...` returns the
  operational read model as deterministic JSON.
- `GET /healthz` provides a process-level health response.

Every response includes an application-generated `X-Request-ID`. The CLI emits
one JSON access record to standard error with that ID, method, path, status, and
request duration. Records deliberately omit query strings, headers, request
bodies, signatures, key IDs, and event data so operational correlation does not
turn the access log into a credential or merchant-data sink.

Run the local-only demo server with a secret supplied through the environment:

```bash
MERCHANT_EVENT_SECRETS='{"local":"local-demo-only"}' \
  python3 -m merchant_event_lab.http --database inventory.sqlite3 --port 8000 \
  --event-rate-limit 60 --rate-window-seconds 60
```

For example, sign the exact body bytes and submit one synthetic restock:

```bash
body='{"event_id":"restock-001","sku":"demo-sku","delta":5}'
timestamp="$(date +%s)"
signature="sha256=$(printf '%s.%s' "$timestamp" "$body" | openssl dgst -sha256 -hmac 'local-demo-only' -hex | cut -d' ' -f2)"
curl -i http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -H 'X-Event-Key-Id: local' \
  -H "X-Event-Timestamp: $timestamp" \
  -H "X-Event-Signature: $signature" \
  --data-binary "$body"
curl -s 'http://127.0.0.1:8000/inventory?low_stock_at=5'
```

The bundled `wsgiref` server is for a local demonstration, not production. The
prototype does not provide TLS termination, secret-manager integration,
distributed rate limiting, metrics, tracing, or durable log shipping. Its
single global in-process rate bucket is intentionally narrow: restarts reset it
and multiple workers do not share state. Production deployments should enforce
distributed tenant-aware limits at a trusted edge, with careful proxy identity handling.
Production logging should use a bounded, non-blocking sink with retention and
redaction controls rather than writing synchronously to standard error.
