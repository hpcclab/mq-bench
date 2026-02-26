# mq-bench — AI agent working notes

Purpose
- Multi-transport messaging benchmark harness (MQTT, Redis, NATS, Zenoh, RabbitMQ/AMQP) with pub/sub and req/rep, CSV metrics, and Dockerized brokers.

Architecture (big picture)
- One binary with subcommands (`src/main.rs`): pub, sub, req, qry, plus multi-topic `mt-pub`/`mt-sub`.
- Transport abstraction (`src/transport`): `Engine` = zenoh|mqtt|redis|nats|rabbitmq (alias amqp). Builder (`TransportBuilder::connect`) dispatches behind cargo features.
- Subscribe/query handlers: adapters use handler callbacks for lower overhead; publishers are pre-declared for hot-path publish.
- Payload format (`src/payload.rs`): first 24 bytes are header [seq: u64, timestamp_ns: u64, payload_size: usize] in little-endian; subscribers decode for latency.
- Metrics (`metrics::stats`): hdrhistogram-backed; main externalizes snapshots with a shared `Stats` aggregator and periodic writer. CSV via `src/output.rs`.
- Logging: `tracing-subscriber` via `--log-level`.

Services & topology (docker-compose.yml)
- MQTT: Mosquitto 1883, EMQX 1884, HiveMQ 1885, RabbitMQ-MQTT 1886, Artemis-MQTT 1887; Redis 6379; NATS 4222; RabbitMQ AMQP 5672; Artemis AMQP 5673; optional Zenoh 3-router star (7448→7447←7449).
- Zenoh cross-router sanity: start subscriber against 7448 (router2) and publisher to 7449 (router3). Discovery is disabled; use explicit endpoints.

CLI patterns (current)
- Top-level: `--run-id`, `--out-dir`, `--log-level`, `--snapshot-interval`.
- Engine selection: `--engine zenoh|mqtt|redis|nats|rabbitmq` with `--connect KEY=VALUE` (repeatable). Zenoh back-compat: `--endpoint tcp/host:port`.
- QoS: `--qos` accepted on pub/sub/qry; mapped per engine (e.g., zenoh 0=best-effort, 1/2=reliable).
- CSV output: any role accepts `--csv path/to/file.csv`; stdout if omitted (aggregated snapshots handled in `main.rs`).

Quick examples (see README for more):
- MQTT: `sub --engine mqtt --connect host=127.0.0.1 --connect port=1883 --expr bench/topic` | `pub --engine mqtt --connect host=127.0.0.1 --connect port=1883 --topic-prefix bench/topic --payload 200 --rate 5 --duration 10`
- NATS: `sub --engine nats --connect host=127.0.0.1 --connect port=4222 --expr bench/topic` | `pub --engine nats --connect host=127.0.0.1 --connect port=4222 ...`
- Redis: `--engine redis --connect url=redis://127.0.0.1:6379`
- Zenoh: `--endpoint tcp/127.0.0.1:7448` (sub) and `:7449` (pub)
- RabbitMQ AMQP: `--engine rabbitmq --connect url=amqp://guest:guest@127.0.0.1:5672/%2f`

Developer workflows
- Build: `cargo build --release` (prefer release for perf). Tests: `cargo test` (see `tests/mock_transport_smoke.rs`).
- Bring up brokers: `docker compose up -d` (all bound to 127.0.0.1). Tear down: `docker compose down`.
- Scripts (`scripts/`):
  - `run_baseline.sh`, `run_fanout.sh` (set `ENGINE=zenoh|mqtt|redis|nats|rabbitmq`).
  - `lib.sh::make_connect_args` turns `ENGINE` + env into `--engine ... --connect KEY=VALUE` pairs.
  - Optional Docker stats sampler writes `docker_stats.csv` (see `resolve_monitor_containers`, `start_broker_stats_monitor`).

Project conventions
- Topic names use slash style at the CLI (`bench/topic`, `bench/**`); adapters map to backend-native patterns (e.g., NATS dots, AMQP topic exchange).
- Discovery is off for determinism; pass explicit connection params.
- Payload must be ≥ 24 bytes; header is required for latency math.
- Subscriber-first startup avoids drops at high rates.

Key files to know
- CLI/dispatch: `src/main.rs`; roles: `src/roles/{publisher,subscriber,requester,queryable,multi_topic}.rs`
- Transport contracts: `src/transport/{mod,config}.rs` and per-adapter files (mqtt, nats, redis, amqp, zenoh)
- Metrics/CSV: `src/metrics/stats.rs`, `src/output.rs`; rate control: `src/rate.rs`; time sync: `src/time_sync.rs`
- Docker/services: `docker-compose.yml`, `config/*` (Zenoh routers, MQTT broker configs)

Adding a new transport (concise checklist)
- Implement `src/transport/<name>.rs` with `Transport` trait (support subscribe + create_publisher; `request`/`register_queryable` can be stubbed initially).
- Wire it: add `Engine::<Name>` and builder branch in `src/transport/mod.rs` behind cargo feature `transport-<name>`; map CLI in `src/transport/config.rs::parse_engine`.
- Cargo.toml: add feature and deps. Scripts: extend `scripts/lib.sh::make_connect_args` and (optionally) add a service to `docker-compose.yml`.
- Docs: add connect hints and examples in `README.md` under “Transports (current)”.

Pitfalls
- Wrong port → wrong broker/router; check compose port map. Zenoh: 7447/7448/7449 hub/star. MQTT variants: 1883–1887. NATS 4222. Redis 6379. RabbitMQ 5672.
- Debug builds saturate CPU at high rates; use `--release`.
- Some adapters may not yet support req/rep; see README notes.

Questions for maintainers (to confirm/extend):
- Any engine-specific QoS/retention/ack nuances we should encode into adapter defaults?
- Should scripts default `ENGINE` to a specific broker for CI? Any preferred artifact layout under `./artifacts` beyond current run folders?
