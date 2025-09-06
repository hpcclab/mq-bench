# mq-bench — AI agent working notes

Purpose
- Rust benchmarking harness for Zenoh 1.5.1 with a Docker 3-router star topology. Focus: pub/sub now; requester/queryable stubs exist.

Architecture at a glance
- One binary with subcommands (see `src/main.rs`):
  - pub/sub implemented via `roles::publisher` and `roles::subscriber`
  - req/qry placeholders in `roles/{requester,queryable}.rs`
- Zenoh client: explicit endpoints only; discovery disabled in both client and routers.
- Payloads: first 24 bytes are a custom header (see `src/payload.rs`):
  - seq: u64, timestamp_ns: u64, payload_size: usize; little-endian
  - Subscribers parse this to compute end-to-end latency.
- Rate control: open-loop scheduler (`src/rate.rs`) using fixed nanos interval; no backpressure.
- Metrics: `metrics::stats` with hdrhistogram; periodic snapshots printed; CSV writer in `src/output.rs`.
- Logging: `tracing-subscriber` configured via `--log-level` string (e.g., info, debug).

Docker/routers
- Compose spins a 3-router star (`docker-compose.yml`):
  - router1 hub exposed at 7447; router2 at 7448→7447; router3 at 7449→7447
  - Router configs in `config/router{1,2,3}.json5` disable scouting and set explicit peers.
- Typical cross-router sanity: sub connects to 7448 (router2), pub to 7449 (router3). Ensure subscriber starts first.

Developer workflows
- Build binary: `cargo build --release` (or debug for quick runs).
- Bring up routers only: `scripts/compose_up.sh`; tear down: `scripts/compose_down.sh`.

CLI patterns (current)
- Publisher: `mq-bench pub --endpoint tcp/127.0.0.1:7449 --topic-prefix bench/topic --payload 1024 --rate 1000 --duration 60`
- Subscriber: `mq-bench sub --endpoint tcp/127.0.0.1:7448 --expr bench/topic`
- Notes:
  - `--endpoint` is a Zenoh locator like `tcp/host:port` (no discovery).
  - `payload` must be ≥ 24 bytes (header); else it will panic.
  - CSV output is currently to stdout; file paths are coded but not yet exposed via CLI.

Code conventions and extension tips
- When adding a role:
  - Define a Config struct, open a Zenoh session with `connect/endpoints`, disable `scouting/*`, and spawn a snapshot task using `metrics::Stats` every N seconds.
  - Use `tokio::select!` with `signal::ctrl_c()` for graceful shutdown.
- Use `payload::MessageHeader` for latency; prefer little-endian and keep header size fixed at 24 bytes for compatibility.
- Keep router auto-discovery off for determinism; prefer explicit `connect/endpoints` arrays.

Key files
- CLI and dispatch: `src/main.rs`
- Roles: `src/roles/{publisher,subscriber}.rs` (req/qry stubs present)
- Payload header: `src/payload.rs`
- Metrics & CSV: `src/metrics/stats.rs`, `src/output.rs`
- Docker topology: `docker-compose.yml`, `config/router*.json5`
- Example scripts: `test_pubsub.sh`, `debug_test.sh`, `cross_router_test.sh`, `scripts/compose_{up,down}.sh`

Pitfalls
- Wrong host port → wrong router. Use 7447 (router1), 7448 (router2), 7449 (router3).
- Start subscriber before publisher to avoid initial drops.
- High rates in debug builds can saturate CPU; prefer `--release` for performance work.
