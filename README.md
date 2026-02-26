# mq-bench — Messaging transport benchmarking (MQTT + Redis + NATS + Zenoh + RabbitMQ/AMQP)

A minimal, scriptable benchmarking tool for pub/sub and request/reply with CSV metrics. Built in Rust with Tokio. Ships a Docker Compose stack with MQTT (Mosquitto/EMQX/HiveMQ), Redis, NATS, RabbitMQ, ActiveMQ Artemis, and an optional Zenoh 3-router star.

The harness uses a pluggable transport abstraction with adapters for MQTT, Redis, NATS, Zenoh (1.5.1), and RabbitMQ (AMQP via `lapin`). Select an engine via `--engine` and pass connection options via `--connect KEY=VALUE`.

## Features
- Docker Compose stack: MQTT (Mosquitto/EMQX/HiveMQ), Redis, NATS, RabbitMQ, ActiveMQ Artemis, plus an optional Zenoh 3-router star (router2 → router1 ← router3)
- Roles: pub, sub, req, qry; plus multi-topic mt-pub/mt-sub
- Open-loop publisher with rate control
- Requester with QPS pacing, concurrency, and timeouts
- Subscriber with latency measurement (ns timestamps embedded in payload)
- CSV snapshots to stdout or file (`--csv`)

Performance-focused implementation details
- Handler-based subscribe and query registration (lower overhead than streams)
- Reusable declared publisher for hot-path sends
- Payload header is fixed 24 bytes; subscribers decode for E2E latency
- Batching and CSV flushing for live tailing

Transports (current)
- mqtt, redis, nats, zenoh, rabbitmq (AMQP)
  - rabbitmq: native AMQP adapter (alias: `amqp`). Connect via `--connect url=amqp://user:pass@host:port/%2f`.
  - rabbitmq-mqtt: use RabbitMQ’s MQTT plugin through the MQTT adapter (host: 127.0.0.1, port: 1886 by default).
  - artemis: alias to MQTT adapter to hit Artemis’s MQTT listener (127.0.0.1:1887). You can also use `mqtt-artemis` transport.
  - Broker-specific MQTT aliases are available: `mqtt-mosquitto`, `mqtt-emqx`, `mqtt-hivemq`, `mqtt-rabbitmq`, `mqtt-artemis`.
  - Wildcards: use slash style in CLI (e.g., `bench/**`). Adapters map to engine-native patterns (e.g., NATS `a.>`; AMQP uses `amq.topic` with dot routing internally).
  - Topics: use slash form in CLI (e.g., `bench/topic`); adapters map as needed (e.g., NATS and AMQP use dots under the hood).

## Quick start

1) Bring up services (MQTT/Redis/NATS; Zenoh routers optional)

```bash
# From repo root
docker compose up -d
# Verify ports (bound to 127.0.0.1)
# Redis: 6379 | NATS: 4222 | MQTT: 1883/1884/1885 | RabbitMQ: AMQP 5672, MQTT 1886 | Artemis: AMQP 5673, MQTT 1887 | Zenoh: 7447/7448/7449 (optional)
```

2) Build the harness

```bash
cargo build --release
```

3) Run quick pub/sub sanity tests

MQTT (defaults to Mosquitto on 1883)
```bash
./target/release/mq-bench sub --engine mqtt --connect host=127.0.0.1 --connect port=1883 --expr bench/topic
./target/release/mq-bench pub --engine mqtt --connect host=127.0.0.1 --connect port=1883 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

Redis
```bash
./target/release/mq-bench sub --engine redis --connect url=redis://127.0.0.1:6379 --expr bench/topic
./target/release/mq-bench pub --engine redis --connect url=redis://127.0.0.1:6379 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

NATS (default port 4222)
```bash
./target/release/mq-bench sub --engine nats --connect host=127.0.0.1 --connect port=4222 --expr bench/topic
./target/release/mq-bench pub --engine nats --connect host=127.0.0.1 --connect port=4222 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

Zenoh (optional cross-router: sub on router2/7448, pub to router3/7449)
```bash
./target/release/mq-bench sub --endpoint tcp/127.0.0.1:7448 --expr bench/topic
./target/release/mq-bench pub --endpoint tcp/127.0.0.1:7449 --payload 200 --rate 5 --duration 10
```

You should see subscriber logs like:
```
DEBUG: Received message seq=12, latency=1.10ms
Subscriber stats - Received: 20, Errors: 0, Rate: 4.00 msg/s, P99 Latency: 3.01ms
```

## CLI overview

Top-level:
- `--run-id STRING` tag outputs/CSV (optional)
- `--out-dir PATH` base artifacts directory (default `./artifacts`)
- `--log-level trace|debug|info|warn|error` (default `info`)
- `--snapshot-interval SECS` stats snapshot cadence (default `1`)

All roles accept a transport engine and connection options:
- `--engine zenoh|mqtt|redis|nats|rabbitmq` (alias: `amqp`)
- `--connect KEY=VALUE` (repeatable)
- Zenoh back-compat: `--endpoint tcp/host:port` maps to `--connect endpoint=...`

- Publisher (pub)
  - Topic/key selection
    - `--topic-prefix` base key (defaults to `bench/topic`)
    - Multi-topic: `--topics N` and `--publishers M` to create M logical publishers over N topics
  - `--payload` size in bytes
    - `--rate` messages per second (omitted or <= 0 means unlimited)
    - `--duration` seconds
  - `--csv path/to/pub.csv` to write CSV snapshots to a file (stdout if omitted)

- Subscriber (sub)
  - `--expr` key expression, e.g. `bench/topic` or `bench/**`
  - `--csv path/to/sub.csv` to write CSV snapshots to a file (stdout if omitted)

- Requester (req)
  - `--key-expr` query key expression
  - `--qps` queries per second (omitted or <= 0 means unlimited)
  - `--concurrency` max in-flight
  - `--timeout` per-query timeout (ms)
  - `--duration` seconds
  - `--csv path/to/req.csv`

- Queryable (qry)
  - `--serve-prefix` repeatable; prefixes to serve
  - `--reply-size` reply body size in bytes
  - `--proc-delay` processing delay per query (ms)
  - `--csv path/to/qry.csv`

- Multi-topic publisher (mt-pub)
  - `--topic-prefix` base key (e.g., `bench/topic`)
  - Dimensions: `--tenants T --regions R --services S --shards K`
  - `--publishers M` logical publishers (<= T*R*S*K; -1 uses total keys)
  - `--mapping mdim|hash` key mapping across publishers
  - `--payload`, `--rate`, `--duration`, `--csv`, `--share-transport`

- Multi-topic subscriber (mt-sub)
  - `--topic-prefix` base key (e.g., `bench/mtopic`)
  - Dimensions: `--tenants T --regions R --services S --shards K`
  - `--subscribers N` number of per-key subscriptions (<= total keys; -1 uses total keys)
  - `--mapping mdim|hash`, `--duration`, `--csv`, `--share-transport`

Tip: If you pass `--csv ./artifacts/run1/pub.csv` or `sub.csv`, parent directories will be created automatically.

Engine connect hints:
- MQTT: `--connect host=127.0.0.1 --connect port=1883`
  - Optional auth: add `--connect username=USER --connect password=PASS`
- Redis: `--connect url=redis://127.0.0.1:6379`
- NATS: `--connect host=127.0.0.1 --connect port=4222`
- Zenoh: `--endpoint tcp/127.0.0.1:7448` (or `--connect endpoint=tcp/127.0.0.1:7448`)
- RabbitMQ (AMQP): `--connect url=amqp://guest:guest@127.0.0.1:5672/%2f`
- RabbitMQ (MQTT plugin): use MQTT engine at host 127.0.0.1 port 1886
- Artemis (MQTT): use MQTT engine at host 127.0.0.1 port 1887

## Quick request/reply test (examples)

With services up and the binary built:

1) Start a queryable on Zenoh router2 (optional, 7448):

```bash
./target/release/mq-bench qry --endpoint tcp/127.0.0.1:7448 --serve-prefix bench/topic --reply-size 256 --proc-delay 0
```

2) Run a requester on Zenoh router3 (7449):

```bash
./target/release/mq-bench req --endpoint tcp/127.0.0.1:7449 --key-expr bench/topic --qps 1000 --concurrency 32 --timeout 2000 --duration 5
```

NATS
```bash
# Start a responder (subscribe and reply)
./target/release/mq-bench qry --engine nats --connect host=127.0.0.1 --connect port=4222 \
  --serve-prefix bench/topic --reply-size 256 --proc-delay 0

# Run a requester
./target/release/mq-bench req --engine nats --connect host=127.0.0.1 --connect port=4222 \
  --key-expr bench/topic --qps 1000 --concurrency 32 --timeout 2000 --duration 5
```

Redis (simple list-based req/rep baseline)
```bash
./target/release/mq-bench qry --engine redis --connect url=redis://127.0.0.1:6379 \
  --serve-prefix bench/topic --reply-size 256 --proc-delay 0

./target/release/mq-bench req --engine redis --connect url=redis://127.0.0.1:6379 \
  --key-expr bench/topic --qps 1000 --concurrency 32 --timeout 2000 --duration 5
```

Notes:
- Start the queryable before the requester.
- Add `--csv path/to/req.csv` or `qry.csv` to save snapshots.
- RabbitMQ AMQP: request/reply is not implemented yet in the adapter; pub/sub is supported.

## Multi-topic quick test

Publish over many keys and subscribe to all of them in one process:

```bash
# Subscriber over many keys
./target/release/mq-bench mt-sub --engine mqtt --connect host=127.0.0.1 --connect port=1883 \
  --topic-prefix bench/mtopic --tenants 4 --regions 2 --services 3 --shards 8 \
  --subscribers -1 --duration 10

# Matching publisher
./target/release/mq-bench mt-pub --engine mqtt --connect host=127.0.0.1 --connect port=1883 \
  --topic-prefix bench/mtopic --tenants 4 --regions 2 --services 3 --shards 8 \
  --publishers -1 --payload 256 --rate 1000 --duration 10
```

## Topology & services

Docker Compose defines optional services (all bound to 127.0.0.1):
- MQTT brokers: Mosquitto 1883, EMQX 1884, HiveMQ 1885, RabbitMQ MQTT 1886, Artemis MQTT 1887
- RabbitMQ: AMQP 5672
- Artemis: AMQP 5673
- Redis: 6379
- NATS: 4222
- Zenoh star (optional): 7447; 7448→7447; 7449→7447

Configs in `config/` have discovery disabled to ensure deterministic static peering.

## Scenario scripts

- Generic, multi-transport:
  - `scripts/run_baseline.sh` (ENGINE=zenoh|mqtt|redis|nats|rabbitmq)
  - `scripts/run_fanout.sh` (ENGINE=zenoh|mqtt|redis|nats|rabbitmq)
- Convenience wrappers:
  - Baseline: `run_mqtt_baseline.sh`, `run_redis_baseline.sh`, `run_nats_baseline.sh`, `run_rabbitmq_baseline.sh`, `run_artemis_baseline.sh`
  - Fanout: `run_mqtt_fanout.sh`, `run_redis_fanout.sh`, `run_nats_fanout.sh`, `run_rabbitmq_fanout.sh`, `run_artemis_fanout.sh`

Examples for RabbitMQ and Artemis:

RabbitMQ (AMQP)
```bash
./target/release/mq-bench sub --engine rabbitmq --connect url=amqp://guest:guest@127.0.0.1:5672/%2f --expr bench/topic
./target/release/mq-bench pub --engine rabbitmq --connect url=amqp://guest:guest@127.0.0.1:5672/%2f --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

RabbitMQ (MQTT plugin)
```bash
./target/release/mq-bench sub --engine mqtt --connect host=127.0.0.1 --connect port=1886 --expr bench/topic
./target/release/mq-bench pub --engine mqtt --connect host=127.0.0.1 --connect port=1886 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

Artemis (MQTT)
```bash
./target/release/mq-bench sub --engine mqtt --connect host=127.0.0.1 --connect port=1887 --expr bench/topic
./target/release/mq-bench pub --engine mqtt --connect host=127.0.0.1 --connect port=1887 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10
```

## Contributing: add a new transport

Minimal steps to introduce a new engine (e.g., "foo"):

- Adapter: create `src/transport/foo.rs` that implements `Transport` (must support `subscribe` and `create_publisher`; `request`/`register_queryable` can return NotImplemented initially). Parse `--connect KEY=VALUE` from `ConnectOptions.params` and pass payload bytes through unchanged.
- Wire-up in code:
  - `src/transport/mod.rs`: add `Engine::Foo` and call `foo::connect(...)` under a `#[cfg(feature = "transport-foo")]` gate.
  - `src/transport/config.rs`: map engine strings (e.g., "foo") in `parse_engine` to `Engine::Foo`.
  - `Cargo.toml`: add feature `transport-foo` enabling the adapter’s deps.
- Scripts: in `scripts/lib.sh`, teach `make_connect_args` how to turn `ENGINE=foo` into `--engine foo --connect ...` pairs; optionally add cases in `scripts/orchestrate_benchmarks.sh` for baseline/fanout labels.
- Docker (optional): add a service to `docker-compose.yml` and bind ports to 127.0.0.1.
- Docs: list the engine under “Transports (current)” with example connect hints.

Tip: keep topic names in slash style at the CLI; if the backend uses a different pattern (e.g., dots), translate internally inside the adapter.

# mq-bench
Benchmarking of IoT-based message queuing systems.
