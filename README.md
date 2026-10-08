# Order Matching Engine

[![CI](https://github.com/gtomchuk2005/order-matching-engine/actions/workflows/ci.yaml/badge.svg)](https://github.com/gtomchuk2005/order-matching-engine/actions/workflows/ci.yaml)

A limit order matching engine, run end-to-end: orders arrive over HTTP, get
logged to Kafka, matched by a C++ engine holding the order book in memory,
and pushed back out to WebSocket subscribers as book updates and trades.
Redis holds book snapshots for fast reads and idempotency keys at the edge.

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

## Running it

```bash
git clone <this-repo>
cd order-matching-engine
cp .env.example .env
docker compose up -d
docker compose ps
```

Expect five services: `kafka`, `redis`, `init-topics` (exited 0), `engine`,
and `gateway`. `docker compose logs engine` should show `partition N live
at offset X` for three partitions once caught up.

`docker compose stop` stops everything and keeps all data. `docker compose
down -v` wipes the Kafka volume and Redis irreversibly; clearing Redis
alone does nothing lasting, since the engine replays the log on startup
and rewrites every snapshot.

## Using it

Submit a resting buy:
```bash
curl -X POST localhost:8080/orders \
  -d '{"symbol":"DEMO","side":"buy","price":10020,"qty":8,"client_order_id":"d3"}'
```
```json
{"client_order_id":"d3","order_id":"2c87366370b8416084e3cab5d3092ce3","status":"accepted","symbol":"DEMO"}
```
`order_id` is server-assigned and is what a cancel references.

Read the book after several orders have built it up:
```bash
curl localhost:8080/book/DEMO
```
```json
{"symbol":"DEMO","seq":5,"bids":[[10020,8],[10010,5],[10000,10]],"asks":[[10100,7],[10150,3]]}
```
Bids descend, asks ascend, each level is `[price, qty]` aggregated across every order resting there.

Cancel the resting buy:
```bash
curl -X DELETE localhost:8080/orders/2c87366370b8416084e3cab5d3092ce3
```
```json
{"order_id":"2c87366370b8416084e3cab5d3092ce3","status":"cancel_accepted","symbol":"DEMO"}
```
The 10020 level is gone from the book entirely (the 202 only confirms the cancel is in the log, not that the order was still resting when processed):
```json
{"symbol":"DEMO","seq":6,"bids":[[10010,5],[10000,10]],"asks":[[10100,7],[10150,3]]}
```

With asks resting at 10100 x7 and 10150 x3, a buy of 10 sweeps both levels:
```bash
curl -X POST localhost:8080/orders \
  -d '{"symbol":"DEMO","side":"buy","price":10150,"qty":10,"client_order_id":"sweep1"}'
```
Two trades, at two different prices, same taker — each fill executes at the resting (maker) order's price, not the incoming order's:
```json
{"type":"trade","symbol":"DEMO","seq":7,"maker_id":"2bcf0f5d…","taker_id":"a648c396…","price":10100,"qty":7,"ingress_ts_ns":1791393680546321753}
{"type":"trade","symbol":"DEMO","seq":8,"maker_id":"9cd014ec…","taker_id":"a648c396…","price":10150,"qty":3,"ingress_ts_ns":1791393680546321753}
```

`curl` can't speak WebSocket; watching the live stream needs a client such as `websocat`:
```bash
websocat "ws://localhost:8080/stream?symbol=DEMO"
```
The first frame is always a snapshot, then every trade and delta as it happens:
```json
{"type":"snapshot","symbol":"DEMO","seq":1,"bids":[[10050,10]],"asks":[]}
{"type":"trade","symbol":"DEMO","seq":2,"maker_id":"9b4aa981…","taker_id":"8f2a750e…","price":10050,"qty":4,"ingress_ts_ns":1791270512700293583}
{"type":"delta","symbol":"DEMO","seq":3,"side":"bid","price":10050,"qty":6,"ingress_ts_ns":1791270512700293583}
```
A client applies the snapshot, then each later frame only if its `seq` is exactly `last + 1` — anything else means reconnect for a fresh snapshot.

## Design decisions

- **Partition by symbol** — one engine instance owns a partition's symbols outright, so the matching hot path needs no locks and replay deterministically rebuilds its books.
- **The engine is a pure function of its input log** — no wall clock, no generated ids, no external calls in the matching path; it copies `ingress_ts_ns` from the triggering message, and the Redis snapshot write lives in `main.cpp` outside `Engine::apply`. This is what makes replay deterministic.
- **Prices are integer ticks, never floats** — 1 tick = $0.01 ($100.50 is `10050`), since float equality is unreliable and prices must compare exactly.
- **Deltas are absolute, not incremental** — `qty: 6` means the level is now 6 (`qty: 0` means empty), which makes them idempotent under at-least-once delivery. `seq` is per-symbol and monotonic across every trade and delta.
- **`std::list` per price level, not `std::deque`** — cancel holds an iterator into the list to unlink in O(1); a deque would invalidate it, at the cost of worse cache locality when matching walks a level.

Deliberately out of scope: authentication, market/stop/IOC order types, conflation, exactly-once delivery, and more than one engine instance per partition.

## Development

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build        # 58 tests
```

```bash
cd gateway && go test ./...
```

The engine also runs standalone on newline-delimited JSON over stdin (how
CI tests it without Docker); `KAFKA_BROKERS` switches it to Kafka mode and
`REDIS_HOST` enables snapshot writes. `scripts/replay-check.sh` exercises
Kafka mode and restart recovery. Benchmarks are opt-in behind
`-DBUILD_BENCH=ON`.
