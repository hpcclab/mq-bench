# mq-bench — Zenoh Cluster Stress Testing Harness

A minimal, scriptable benchmarking tool for Zenoh (v1.5.1), built in Rust with a Docker-based 3-router star cluster. Focus: pub/sub and request/reply with CSV metrics.

## Features
- Docker Compose 3-router star topology (TCP): router2 → router1 ← router3
- Rust harness (tokio + zenoh 1.5.1) with CLI roles: pub, sub, req, qry
- Open-loop publisher with rate control
- Requester with QPS pacing, concurrency, and builder-level timeouts
- Subscriber with latency measurement (ns timestamps embedded in payload)
- CSV-style snapshots to stdout (or CSV file via --csv)

## Quick start

1) Bring up the routers

```bash
# From repo root
docker compose up -d
# Verify ports
# router1: 7447, router2: 7448, router3: 7449
```

2) Build the harness

```bash
cargo build --release
```

3) Run a cross-router pub/sub sanity test

- Subscribe on router2 (7448):
```bash
./target/release/mq-bench sub --endpoint tcp/127.0.0.1:7448 --expr bench/topic
```
- Publish on router3 (7449):
```bash
./target/release/mq-bench pub --endpoint tcp/127.0.0.1:7449 --payload 200 --rate 5 --duration 10
```

You should see subscriber logs like:
```
DEBUG: Received message seq=12, latency=1.10ms
Subscriber stats - Received: 20, Errors: 0, Rate: 4.00 msg/s, P99 Latency: 3.01ms
```

## CLI overview (current)

- Publisher (pub)
  - `--endpoint` tcp host:port, e.g. `tcp/127.0.0.1:7449`
  - Topic/key is currently the `topic_prefix` argument in the top-level CLI (defaults to `bench/topic`)
  - `--payload` size in bytes
    - `--rate` messages per second (omitted or <= 0 means unlimited)
    - `--duration` seconds
  - `--csv path/to/pub.csv` to write CSV snapshots to a file (stdout if omitted)

- Subscriber (sub)
  - `--endpoint` tcp host:port, e.g. `tcp/127.0.0.1:7448`
  - `--expr` key expression, e.g. `bench/topic`
  - `--csv path/to/sub.csv` to write CSV snapshots to a file (stdout if omitted)

- Requester (req)
  - `--endpoint` tcp host:port, e.g. `tcp/127.0.0.1:7449`
  - `--key-expr` query key expression
  - `--qps` queries per second (omitted or <= 0 means unlimited)
  - `--concurrency` max in-flight
  - `--timeout` per-query timeout (ms)
  - `--duration` seconds
  - `--csv path/to/req.csv`

- Queryable (qry)
  - `--endpoint` tcp host:port, e.g. `tcp/127.0.0.1:7448`
  - `--serve-prefix` repeatable; prefixes to serve
  - `--reply-size` reply body size in bytes
  - `--proc-delay` processing delay per query (ms)
  - `--csv path/to/qry.csv`

Tip: If you pass `--csv ./artifacts/run1/pub.csv` or `sub.csv`, parent directories will be created automatically.

## Quick request/reply test

With the Docker routers up and the binary built:

1) Start a queryable on router2 (7448):

```bash
./target/release/mq-bench qry --endpoint tcp/127.0.0.1:7448 --serve-prefix bench/topic --reply-size 256 --proc-delay 0
```

2) Run a requester on router3 (7449):

```bash
./target/release/mq-bench req --endpoint tcp/127.0.0.1:7449 --key-expr bench/topic --qps 1000 --concurrency 32 --timeout 2000 --duration 5
```

Notes:
- Start the queryable before the requester.
- Add `--csv path/to/req.csv` or `qry.csv` to save snapshots.

## Large-scale multi-topic test

Goal: stress routing/filtering with many distinct topic prefixes (e.g., thousands of keys) and verify throughput/latency remain reasonable.

Recommended cross-router setup (subscriber on router2, publisher on router3):

1) Start a wildcard subscriber that matches all topic indices:

```bash
./target/release/mq-bench sub \
  --endpoint tcp/127.0.0.1:7448 \
  --expr "bench/topic/**" \
  --csv ./artifacts/run-mtopics/sub_agg.csv
```

2) Run publishers across many topics. With the current CLI, each publisher i uses key `topic_prefix/(i % topics)`. To cover all topics evenly, set `--publishers >= --topics`.

```bash
./target/release/mq-bench pub \
  --endpoint tcp/127.0.0.1:7449 \
  --topic-prefix bench/topic \
  --topics 1000 \
  --publishers 1000 \
  --payload 200 \
  --rate 10 \
  --duration 30 \
  --csv ./artifacts/run-mtopics/pub.csv
```

Notes:
- Start the subscriber first to avoid initial drops.
- If `--publishers < --topics`, only the first `(publishers % topics)` topic indices will be active due to the current mapping.
- Use wildcard expressions on the subscriber (`bench/topic/**`) to receive all indices without spawning per-topic subscribers.
- For very large topic counts, monitor CPU and consider lowering `--rate` per publisher.

## Topology

Docker Compose defines a 3-router star and exposes ports:
- router1: 7447
- router2: 7448 → 7447 in container
- router3: 7449 → 7447 in container

Configs in `config/` have discovery disabled to ensure deterministic static peering.

## Roadmap
- Phase 4: requester/queryable minimal functionality (QPS, timeouts, concurrency)
- Phase 5: run summary JSON + optional Prometheus
- Phase 6: scenario scripts to generate artifacts
- Phase 7: fault injection (router restarts, churn)

## Troubleshooting
- Ensure the right router port: sub on 7448, pub on 7449 for cross-router test
- If messages don’t appear, wait a second; the subscriber must be running before the publisher
- Check container logs: `docker compose logs -f router1`

## License
MIT
