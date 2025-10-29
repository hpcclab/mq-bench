# Task Plan and Instructions (Zenoh Stress Testing)

This checklist organizes work into small, verifiable steps. It complements `DESIGN.md`, `DOCKER_DESIGN.md`, and `RUST_DESIGN.md`. No implementation is included here.

## Phase 0 — Decisions and Inputs

- [x] Pin Zenoh image tag (for routers) and Zenoh crate version (for client).
  - Selected: Zenoh 1.5.1 (Docker image tag and crate version)
- [x] Choose transport(s) to test (TCP; QUIC if applicable).
  - Selected: TCP only
- [x] Pick topologies for first runs (single-router, 3-router star or ring).
  - Selected: 3-router star topology (router2 -> router1, router3 -> router1)
- [x] Metrics backend: CSV-only first; Prometheus optional.
  - Selected: CSV-only (Prometheus disabled for now)

Done when: versions and topology are recorded in `TASKS.md` and referenced in `DOCKER_DESIGN.md`. (Status: DONE)

## Phase 1 — Docker Compose Skeleton (static topology)

- [x] Add `docker-compose.yml` with services: `router1`, `router2`, `router3`, and placeholder client roles.
- [x] Create `config/router1.json5`, `config/router2.json5`, `config/router3.json5` with discovery disabled and static peers.
- [x] Add `scripts/compose_up.sh` and `scripts/compose_down.sh` (optional helpers).
- [x] Validate that routers start and form the intended topology without auto-discovery.

Instructions (later):
- Ensure Docker and Compose are installed.
- Bring up only routers first; inspect logs to confirm peering.

Done when: routers are running and peered via static config; no discovery messages appear in logs. (Status: DONE)

## Phase 2: Rust Harness Scaffold ✓ DONE
- [x] Create Rust harness with Clap CLI ✓ DONE  
- [x] Module structure (roles/, metrics/, etc.) ✓ DONE
- [x] CLI parsing with pub/sub/req/qry subcommands ✓ DONE
- [x] Dependencies: tokio, zenoh 1.5.1, clap, hdrhistogram, etc. ✓ DONE
- [x] Empty stubs compile successfully ✓ DONE

## Phase 3: Publisher/Subscriber Core ✓ DONE
- [x] Implement payload header encode/parse helpers ✓ DONE
- [x] Publisher open-loop scheduler and basic send loop ✓ DONE 
- [x] Publisher functionality working with Docker cluster ✓ DONE
- [x] CSV output with statistics snapshots ✓ DONE
- [x] Subscriber receive loop with latency calculation ✓ DONE
- [x] Shell script for testing pub/sub processes ✓ DONE
- [x] Fix message routing issue (exposed router2/3 ports; verified sub on 7448 and pub on 7449 end-to-end)
- [x] CSV snapshots with latency percentiles working end-to-end (stdout). Note: CSV-to-file supported in code; wiring CLI flags for output paths is next.

## Phase 3 — Publisher/Subscriber Minimal Functionality (Recap)

- [x] Implement payload header encode/parse helpers.
- [x] Implement `pub` open-loop scheduler and basic send loop.
- [x] Implement `sub` receive loop with latency calculation and CSV snapshots.
- [x] Validate end-to-end on multi-router (3-router star) at small rates.

Done when: CSV outputs include throughput and latency stats; runs complete without errors. (Status: DONE)

## Phase R — Transport Abstraction Refactor (for Baselines & Extensibility)

- [x] Introduce a transport abstraction trait to decouple roles from Zenoh specifics.
  - Implemented with handler-based APIs: `subscribe(expr, handler)` and `register_queryable(prefix, handler)` to minimize overhead.
  - Traits for guards/handles: `Subscription`, `Publisher`, `QueryRegistration`.
  - Error model unified in a small enum; adapters map engine-specific errors.
- [x] Extract shared wire/payload logic
  - `payload::MessageHeader` is the canonical 24-byte header (seq, ts, len) and remains fixed for comparability.
  - `Payload` supports zero-copy via Zenoh ZBytes; conversion helpers (`as_cow`, `into_bytes`).
- [x] Adapter: implement `Transport` for Zenoh (feature default-on).
  - Publish hot-path fixed (no per-message declare); reusable declared publisher added.
- [x] Refactor roles (`publisher`, `subscriber`, `requester`, `queryable`) to use the `Transport` trait.
  - Subscriber callback does minimal work and batches metrics in a worker.
  - Queryable uses handler model; returns a guard for clean shutdown.
- [x] CLI: add `--engine` enum with values: `zenoh` (default), `redis`, `zmq`, `tcp` (others later).
  - [x] Add generic `--connect` (repeatable `KEY=VALUE`) to pass engine-specific options.
  - [x] Back-compat: `--endpoint` still works for `zenoh`; translate into `--connect`.
- [x] Features and deps
  - [x] Add Cargo features per engine to keep deps optional. Prioritize: `transport-redis`, `transport-zmq` (move `transport-tcp` to later).
  - [x] Build profiles compile only `zenoh` by default; others via `--features`.
- [ ] Testing
  - Add a mock transport to unit-test roles without a broker.
  - Smoke test: run pub/sub and req/qry with `--engine zenoh` to ensure no regressions.

Done when: the binary runs unchanged for Zenoh (`--engine zenoh` default), unit tests pass, and adding a new engine is limited to an adapter + minimal wiring.

## Phase 4 — Requester/Queryable Minimal Functionality ✓ PARTIAL DONE

- [x] Implement `qry` to serve prefixes and reply with fixed-size payloads.
- [x] Implement `req` to issue queries at QPS with concurrency; capture time-to-first/last, timeouts.
- [x] Optimize requester hot path:
  - Batch stats updates via an internal flume queue + single worker.
  - Use Zenoh get-builder timeouts (no outer tokio timeout).
  - Receive replies via handler-based `.with(flume::bounded())` and accumulate first/last/count.
- [ ] CSV outputs include richer query metrics (qps, time_to_first, time_to_last, timeouts, replies_per_query).
  - Current: time_to_first recorded into latency histogram; timeouts are reflected in `error_count`.

Done when: basic query scenarios run and produce CSV metrics.

## Phase 5 — Metrics, Summaries

- [ ] Add per-second snapshots and run summary JSON with aggregates.
- [ ] Plotting script or notebook to visualize results.

Done when: artifacts directory contains CSVs and summary JSON; optional metrics scrape works.

## Phase 6 — Scenario Scripts (Compose)

- [x] Add `scripts/run_local_baseline.sh` — 1→1 pub/sub sweeps.
- [x] Add `scripts/run_fanout.sh` — 1→N sweeps. (initial implementation added; aggregates N subs and one pub, writes CSVs)
- [x] Add `scripts/run_cluster_3r.sh` — 3-router star/ring scenarios.
- [x] Add `scripts/run_queries.sh` — requester/queryable sweeps. (initial implementation added; supports QPS, concurrency, timeouts, CSVs)

### Multi-topic fanout plan

Objective: Validate behavior with a large number of distinct, multi-segment topic paths (hundreds to thousands) and measure routing/filter performance.

Approach:
- Use multi-segment keys to mimic real workloads, e.g.: `topic_prefix/t{tenant}/r{region}/svc{s}/k{idx}`.
  - Mapping option A (multi-dimensional): choose counts T (tenants), R (regions), S (services), K (shards). For publisher i:
    - `t = i % T`, `r = (i / T) % R`, `s = (i / (T*R)) % S`, `k = (i / (T*R*S)) % K`.
  - Mapping option B (hash-based spread): `h = fnv1a(i)` then `t = h % T`, `r = (h / T) % R`, etc., to reduce predictability.
- Run a single wildcard subscriber (`expr=topic_prefix/**`) to aggregate across all paths.
- Cross-router path (router2 sub, router3 pub) to exercise inter-router routing.
- Sweep parameters:
  - Dimensions: T×R×S×K total keys (examples: T∈{10,50}, R∈{2,5}, S∈{5,10}, K∈{10,50}).
  - Publishers ≈ total keys (or a representative subset).
  - Payload ∈ {200, 1024}; per-publisher rate ∈ {5, 10, 50}; duration 30–60s.

Success criteria:
- No crashes or error spikes; error_count ~ 0.
- Interval throughput scales with topics×rate until constrained by CPU/network.
- Latency p95 stays within an acceptable bound (record and compare across sweeps).

Edges & tips:
- Start subscriber before publisher to avoid initial drops.
- Ensure subscriber uses a wildcard (`/**`) that spans all path segments.
- If publishers < total keys, only a subset of paths will be active; prefer hash-based mapping to sample the space uniformly.
- Consider adding per-path subscribers in a follow-up scenario to measure fanout fairness; for now, use a single wildcard sub for simplicity.
- Implementation note: To scale to thousands of paths without spawning thousands of OS processes, add a single-process Rust driver (or extend `pub`) that instantiates many logical publishers and rotates over generated multi-segment keys; aggregate stats in-process.

### Multi-prefix query plan

Objective: Validate query handling across many served, multi-segment prefixes and concurrent requesters.

Approach:
- Start a single queryable serving many prefixes using repeated `--serve-prefix` flags (hundreds to thousands) with depth, e.g.: `bench/qry/t{tenant}/r{region}/ns{ns}`.
- Launch N requesters; each targets a distinct key like `bench/qry/t{t}/r{r}/ns{n}/item/{id}`.
  - Use multi-dimensional or hash-based mapping (as above) to assign (t, r, n, id) and reduce predictability.
- Cross-router runs (queryable on 7448, requesters on 7449) to traverse the star-topology hub.
- Implementation note: Prefer a single-process, multi-requester Rust mode (extend `req` or add a scenario driver) that accepts a key template + dimensions and fans out across many keys internally, avoiding launching hundreds of OS processes; keep one shared Stats/CSV aggregator.

Sweep:
- prefixes (combinations of tenants×regions×namespaces) = [100, 500, 1000]
- requesters = [10, 50, 100]
- qps per requester = [10, 25, 50]
- concurrency per requester = [8, 16, 32]
- reply_size = [200, 1024]
- duration = 30–60s

Success criteria:
- Error rate ~ 0 under target load; timeouts within expected bounds when saturated.
- Queryable “served” count and requester “received” count align within a few percent.
- Stable interval throughput; p95 TTF within acceptable ranges for the test environment.

Done when: scripts can run with routers up and produce artifact folders for each run.

## Phase 10 — Non‑Zenoh Baselines (Comparison Engines)

Plan revision: prioritize MQTT and ZMQ; Redis is de‑prioritized (kept as optional/experimental).

- [ ] MQTT — FIRST TIER (recommended)
  - Client: `rumqttc` (async). Use QoS 0 to match at‑most‑once perf testing.
  - Pub/Sub: topics map 1:1. Retain the 24‑byte header for latency.
  - Req/Rep: emulate via reply topics (e.g., request → `bench/q`, reply → `bench/q/replies/{client_id}/{corr_id}`) or a reply‑to header.
  - CLI: `--connect host=127.0.0.1,port=1883[,client_id=...,username=...,password=...]`.
- [ ] ZeroMQ (ZMQ) — FIRST TIER
  - `zmq` crate; XPUB/XSUB proxy for pub/sub; REQ/REP for queries (pool for concurrency).
  - CLI: `--connect xsub=tcp://HOST:5556,xpub=tcp://HOST:5557` (pub/sub); `req_url=tcp://HOST:5560` requester, `req_bind=tcp://0.0.0.0:5560` queryable.
- [ ] Redis — OPTIONAL / EXPERIMENTAL
  - Already prototyped (pub/sub and LIST‑based req/rep). Keep for occasional comparisons but not a primary baseline.
- [ ] Raw TCP — LATER STAGE (reference ceiling)
  - Length‑prefixed frames over `tokio::net::TcpStream`; simple echo for req/rep.
- [ ] NATS — LATER STAGE
  - `async-nats`; pub/sub and request/reply. CLI: `--connect url=nats://127.0.0.1:4222`.
- [ ] gRPC streaming — LATER STAGE
  - `tonic` bidi‑stream (pub/sub‑like) and unary (req/rep).

Docker & orchestration (first‑tier focus):
- [x] Add `mosquitto` service (1883). Minimal config; persistence off. Keep Zenoh routers unchanged.
- [ ] Add `zmq-proxy` (XSUB 5556, XPUB 5557). Redis service optional.
- [x] Add alternative MQTT brokers for comparison: EMQX (1884) and HiveMQ CE (1885). (Enabled anonymous/allow-all flows for easy testing)

Harness & scripts:
- [x] Add `transport-mqtt` feature and adapter (rumqttc): publish/subscribe implemented; reply-topic req/rep path implemented (validation next).
- [x] Add `scripts/run_mqtt_baseline.sh`, `scripts/run_mqtt_fanout.sh`, `scripts/run_mqtt_queries.sh`.
- [ ] Add `scripts/run_baselines.sh` to sweep engines (`mqtt`, `zmq`; `redis` optional).
- [x] Artifacts: common CSV schema; initial engine-labeled subfolders produced by scripts.

Validation & acceptance:
- [ ] ZMQ: local 1→1 pub/sub and req/qry runs complete without errors at small rates.
- [x] MQTT: local 1→1 pub/sub runs complete without errors (Mosquitto, EMQX, HiveMQ CE tested).
- [ ] MQTT: req/qry scenario validated end-to-end (reply-topic path) at small rates.
- [ ] CSVs comparable (headers/units aligned); sanity‑check vs Zenoh.
- [ ] README note on how to bring up each service and run.

Broker choice (MQTT):
- Recommended: Eclipse Mosquitto
  - Pros: tiny footprint, stable, ubiquitous Docker image, straightforward config; ideal for QoS0 throughput baselines.
  - Docker: `eclipse-mosquitto:2` (ports 1883, optional 8883 for TLS).
- Alternatives:
  - EMQX (Open Source): richer features, dashboards; heavier but still solid for load testing.
  - HiveMQ CE: reliable, good tooling; non‑commercial feature limits.
  - For apples‑to‑apples perf and simplicity, start with Mosquitto.

## Phase 7 — Fault Injection

- [ ] Add `scripts/faults.sh` to restart routers, churn clients.
- [ ] Add optional `netem` helpers to introduce latency/jitter/loss (requires CAP_NET_ADMIN).
- [ ] Document safe use and cleanup.

Done when: selected scenarios run with faults; behavior and recovery are observable in metrics.

## Phase 8 — Reporting

- [ ] Create a minimal analysis script or notebook to plot latency CDFs and throughput vs latency.
- [ ] Add a `REPORTING.md` with steps to generate and read graphs.

Done when: a small report can be generated from a run folder.

## Phase 9 — CI (optional)

- [ ] Add a GitHub Action (or other CI) to build the harness and run a tiny smoke test (local only).
- [ ] Archive artifacts for inspection.

Done when: CI runs on PRs and fails on regressions or build errors.

## File structure (to be created incrementally)

```
config/
  router1.json5
  router2.json5
  router3.json5
scripts/
  compose_up.sh
  compose_down.sh
  run_local_baseline.sh
  run_fanout.sh
  run_cluster_3r.sh
  run_queries.sh
  faults.sh
artifacts/           # output (gitignored)
```

## Guidance and tips

- Keep routers up while iterating on client parameters to reduce churn.
- Start with small message sizes and rates; confirm metrics correctness before scaling.
- Use fixed seeds/run IDs and capture the exact command/params in the summary JSON for reproducibility.
- Prefer CSV-first; add Prometheus only after CSV is validated.

## Acceptance checklist (initial)

- [x] Deterministic Docker topologies with discovery disabled (Zenoh routers; MQTT brokers use explicit ports; no discovery).
- [x] Harness can run pub/sub and req/qry with CSV outputs.
- [x] Minimal scenario scripts produce artifacts consistently.
- [ ] Fault injection does not crash the system; metrics capture degradation and recovery.
- [x] Transport abstraction in place; Zenoh + at least one non‑Zenoh engine (MQTT, Redis) runs the same scenarios.

## Open items

- Confirm Zenoh versions/tags.
- Decide on enabling Prometheus by default.
- Define 2–3 hard SLOs for key scenarios (baseline and fanout).

Notes for next iteration:
- Validate MQTT req/rep end-to-end (reply-topic envelope) and extend CSV for requester/queryable: TTF, TTL, timeouts, replies_per_query.
- Wire a `scripts/run_baselines.sh` to sweep engines and scenarios; include per-engine subfolders consistently.
- Add ZMQ adapter and dockerized XPUB/XSUB proxy; implement pub/sub and req/rep.
- Add per-run summary JSON with aggregates; keep per-second snapshots.
- Faults: add restart/churn helpers and optional netem.
