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

## Phase 4 — Requester/Queryable Minimal Functionality

- [ ] Implement `qry` to serve prefixes and reply with fixed-size payloads.
- [ ] Implement `req` to issue queries at QPS with concurrency; capture time-to-first/last, timeouts.
- [ ] CSV outputs include query metrics (qps, ttf, ttl, timeouts, replies_per_query).

Done when: basic query scenarios run and produce CSV metrics.

## Phase 5 — Metrics, Summaries, Prometheus (optional)

- [ ] Add per-second snapshots and run summary JSON with aggregates.
- [ ] Optional: add Prometheus endpoint and a minimal dashboard config.

Done when: artifacts directory contains CSVs and summary JSON; optional metrics scrape works.

## Phase 6 — Scenario Scripts (Compose)

- [ ] Add `scripts/run_local_baseline.sh` — 1→1 pub/sub sweeps.
- [ ] Add `scripts/run_fanout.sh` — 1→N sweeps.
- [ ] Add `scripts/run_cluster_3r.sh` — 3-router star/ring scenarios.
- [ ] Add `scripts/run_queries.sh` — requester/queryable sweeps.

Done when: scripts can run with routers up and produce artifact folders for each run.

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

- [ ] Deterministic Docker topologies with discovery disabled.
- [ ] Harness can run pub/sub and req/qry with CSV outputs.
- [ ] Minimal scenario scripts produce artifacts consistently.
- [ ] Fault injection does not crash the system; metrics capture degradation and recovery.

## Open items

- Confirm Zenoh versions/tags.
- Decide on enabling Prometheus by default.
- Define 2–3 hard SLOs for key scenarios (baseline and fanout).

Notes for next iteration:
- Wire CLI flags for CSV output files (pub/sub) and run IDs; default to stdout if not provided.
- Add simple scenario scripts and an artifacts folder structure for runs.
