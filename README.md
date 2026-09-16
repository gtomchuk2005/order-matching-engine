# Order Matching Engine

A limit order matching engine, run end-to-end: orders arrive over HTTP, get
logged to Kafka, matched by a C++ engine holding the order book in memory,
and pushed back out to WebSocket subscribers as book updates and trades.
Redis holds book snapshots for fast reads and idempotency keys at the edge.

## What is a matching engine?

The **order book** holds resting limit orders on two sides — **bids**
(buys) and **asks** (sells) — each side sorted by price.

```
  ASKS (sell)   101.20
                101.10   ← lowest ask = "best ask"
               ─────────  spread
                100.90   ← highest bid = "best bid"
  BIDS (buy)    100.80
```

The **best bid** is the highest buy, the **best ask** the lowest sell —
the most competitive orders on each side, sitting adjacent to the spread
(most competitive meaning first in line to trade, not most profitable for
whoever placed it). An incoming order matches when it crosses the opposite side's best price,
consuming resting quantity; any unmatched remainder rests in the book.
Orders at the same price fill in arrival order (**price-time priority**).

## Architecture

```
  POST /orders                                    WS /stream?symbol=AAPL
  DELETE /orders/{id}                                       ▲
        │                                                   │
        ▼                                                   │
  ┌──────────────┐                                 ┌────────┴──────────┐
  │ Go gateway   │                                 │ Go gateway        │
  │ - validate   │                                 │ - subscriber hub  │
  │ - idempotency│                                 │ - push deltas     │
  └──────┬───────┘                                 └────────▲──────────┘
         │                                                  │
         │ SETNX    ┌─────────────── Kafka ──────────────┐  │
         │          │  ┌──────────┐        ┌──────────┐  │  │
         ├─produce─►│  │  orders  │        │  deltas  │──┼──┘
         │          │  └────┬─────┘        └────▲─────┘  │
         ▼          └───────┼───────────────────┼────────┘
   ┌─────────┐              │ consume           │ produce
   │  Redis  │              ▼                   │
   │ - idem  │       ┌──────────────────────────┴───┐
   │ - book  │◄──────┤        C++ engine            │
   └─────────┘ snap  │ - order book, match, cancel  │
                     └──────────────────────────────┘
```

## Message formats

JSON message types flow between the Go gateway, the C++ engine, and
Redis: `Order`, `Cancel`, and `Amend` (gateway → `orders` topic), `Trade`
and `Delta` (engine → `deltas` topic), and `Snapshot` (engine → Redis,
key `book:{symbol}`). The engine itself reads newline-delimited JSON on
stdin, one message per line, writes newline-delimited `Trade` and
`Delta` events to stdout, and writes a full `Snapshot` to Redis after
every message that changes a book.

Prices are integer ticks — 1 tick = $0.01, so $100.50 is 10050.

```json
// Order
{"type": "new", "symbol": "AAPL", "order_id": "a1", "side": "buy",
 "price": 10050, "qty": 10, "ingress_ts_ns": 1755273600123456789}
```

```json
// Cancel
{"type": "cancel", "symbol": "AAPL", "order_id": "a1",
 "ingress_ts_ns": 1755273600123456789}
```

```json
// Amend
{"type": "amend", "symbol": "AAPL", "order_id": "a1", "price": 10100,
 "qty": 5, "ingress_ts_ns": 1755273600123456789}
```

```json
// Trade
{"type": "trade", "symbol": "AAPL", "seq": 1, "maker_id": "a1",
 "taker_id": "b7", "price": 10050, "qty": 4,
 "ingress_ts_ns": 1755273600123456789}
```

```json
// Delta
{"type": "delta", "symbol": "AAPL", "seq": 2, "side": "bid",
 "price": 10050, "qty": 6, "ingress_ts_ns": 1755273600123456789}
```

```json
// Snapshot (Redis key: book:AAPL)
{"symbol": "AAPL", "seq": 42,
 "bids": [[10050, 6], [10049, 25]],
 "asks": [[10052, 8], [10053, 40]]}
```

A `qty: 0` on a `Delta` means the level is now empty. `seq` is per-symbol
and monotonic across every `Trade` and `Delta` emitted for that symbol.
The engine copies `ingress_ts_ns` from the message that caused the event
— it never generates a timestamp of its own.

## Getting started

### Prerequisites

**Required**

| Tool | Minimum version | Needed for | Install |
|---|---|---|---|
| Docker | Desktop or Colima, any recent version | Running the entire system | https://docs.docker.com/get-started/get-docker/ |

**Optional** — for building or testing outside Docker

| Tool | Minimum version | Needed for | Install |
|---|---|---|---|
| Go | 1.26+ | Building and testing the gateway outside Docker | https://go.dev/doc/install |
| CMake | 3.20+ | Building and testing the engine outside Docker | https://cmake.org/download/ |

Every component — Kafka, Redis, the engine, and the gateway — runs in a
container, so Docker alone runs the whole system.

### Quick start

```bash
git clone <this-repo>
cd order-matching-engine
cp .env.example .env
docker compose up -d
```

`.env` holds local configuration and is gitignored — edit it freely.

This starts Kafka and Redis, then runs a
one-shot `init-topics` job that creates the `orders` and `deltas`
topics (idempotent — safe to rerun).

### Verify it's up

```bash
docker compose ps
```

Expected — all three services healthy or exited cleanly:

```
NAME                            STATUS
matching-engine-kafka           Up (healthy)
matching-engine-redis           Up (healthy)
matching-engine-init-topics     Exited (0)
```

```bash
# List topics — expect "orders" and "deltas"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Redis responds — expect "PONG"
docker compose exec redis redis-cli ping
```

If you have `redis-cli` installed locally, it can talk to the containerized
Redis directly via the published host port: `redis-cli -p 6379 ping`.

### Running the engine

Dependencies are fetched and built by CMake — nothing to install beyond
CMake and a C++20 compiler.

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

The engine reads orders on stdin and writes events to stdout:

```bash
./build/engine_main <<'EOF'
{"type":"new","symbol":"AAPL","order_id":"a1","side":"buy","price":10000,"qty":10,"ingress_ts_ns":1}
{"type":"new","symbol":"AAPL","order_id":"a2","side":"sell","price":10000,"qty":4,"ingress_ts_ns":2}
EOF
```

Setting `REDIS_HOST` additionally writes a book snapshot to
`book:{symbol}` after every message that changes a book. `REDIS_PORT`
defaults to 6379. With `REDIS_HOST` unset the engine is stdin-to-stdout
only, which is how CI tests it without Docker.

```bash
REDIS_HOST=127.0.0.1 ./build/engine_main < orders.ndjson
redis-cli -p 6379 GET book:AAPL
```

Following the two orders above, 4 shares trade and 6 rest on the bid:

```json
{"symbol":"AAPL","seq":3,"bids":[[10000,6]],"asks":[]}
```

A dead Redis connection is fatal — the engine reports the error and
exits non-zero rather than silently serving stale snapshots.

Setting `KAFKA_BROKERS` runs the engine against Kafka instead of
stdin: it consumes `ORDERS_TOPIC` (default `orders`) as a member of
`KAFKA_GROUP_ID` (default `matching-engine`) and produces trades and
deltas, keyed by symbol, to `DELTAS_TOPIC` (default `deltas`).
Offsets are committed only after the produce is acked. On startup it
replays its partitions from the beginning to rebuild the books, so
`orders` is created with `retention.ms=-1`.

`scripts/replay-check.sh` exercises Kafka mode and restart recovery
against the compose stack.

### Structural overrides

`.env` is yours to edit directly for config values. For structural
changes to `compose.yaml` itself, use `compose.override.yaml` — Compose
auto-loads it when present, and it's gitignored.

## Benchmarks

Order book benchmarks are opt-in via `-DBUILD_BENCH=ON` and are not built
by default:

```bash
cmake -S . -B build -DBUILD_BENCH=ON
cmake --build build --target order_book_bench
./build/order_book_bench
```
