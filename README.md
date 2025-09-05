# mq-bench — Zenoh Cluster Stress Testing Harness

A minimal, scriptable benchmarking tool for Zenoh (v1.5.1), built in Rust with a Docker-based 3-router star cluster. Focus: simple pub/sub first with CSV metrics; request/reply next.

## Features
- Docker Compose 3-router star topology (TCP): router2 → router1 ← router3
- Rust harness (tokio + zenoh 1.5.1) with CLI roles: pub, sub (req/qry planned)
- Open-loop publisher with rate control
- Subscriber with latency measurement (ns timestamps embedded in payload)
- CSV-style snapshots to stdout (file output supported, flag wiring next)

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
  - `--rate` messages per second
  - `--duration` seconds (0 = unlimited)

- Subscriber (sub)
  - `--endpoint` tcp host:port, e.g. `tcp/127.0.0.1:7448`
  - `--expr` key expression, e.g. `bench/topic`

Note: CSV snapshots are printed to stdout. File output is supported internally; wiring CLI for output paths is next.

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
