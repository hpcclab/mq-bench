Zenoh Cluster Stress Test – Simple Design

This document outlines a pragmatic, minimal design to stress-test a Zenoh cluster. It focuses on clarity, repeatability, and small steps. No code is included here.

Goals and scope
- Measure throughput and latency under different loads and topologies.
- Verify stability under scale (many pubs/subs), churn, and simple faults.
- Keep the first iteration small and automatable on one machine or a few VMs.
 - Include Zenoh query (request/reply to storages/queryables) performance and scalability.

Out of scope (for now): full-featured dashboards, complex orchestration, or advanced analyses. These can be added later.

Success criteria
- Baseline: reach a clear sustained throughput for common payload sizes without message loss (reliable mode) and with p99 latency within target.
- Stability: no crashes; predictable degradation under overload; clean recovery after simple failures.
- Reproducibility: same scenario yields similar results across runs.

Targets (example, to be confirmed):
- p99 latency < 50 ms at 1 KB payload with 1→N fanout up to N=10.
- Zero loss on reliable delivery; bounded loss on best-effort when oversubscribed.

Test scenarios (minimal matrix)
- Payload sizes: 100 B, 1 KB, 10 KB, 100 KB.
- Topic counts: 1, 10, 100.
- Patterns:
  - 1→1 baseline
  - 1→N broadcast (N in {10, 100})
  - N→1 aggregation (N in {10, 100})
- Publisher rate per pub: 1k, 10k msg/s (best-effort and reliable variants).
- Duration per run: 2–5 minutes with 10% warm-up and cool-down trimming.

Optional later: N→M mesh, mixed payloads, topic churn.

Query scenarios (minimal matrix)
- Key expression selectivity: exact key vs wildcard (e.g., foo/**).
- Responders: single queryable vs multiple queryables (1, 3, 10).
- Reply pattern: single reply vs multi-reply (aggregate all replies within a timeout window).
- Result size per reply: 100 B, 1 KB, 10 KB, 100 KB.
- Query rate: 100, 1k, 10k qps (per requester, scale with multiple requesters).
- Concurrency: in-flight queries per requester (1, 10, 100).
- Duration: 2–5 minutes with 10% warm-up/cool-down.

Optional later: time-range/storage-backed queries (cold vs warm cache), server-side processing delay injection, pagination/streaming results if applicable.

Topologies
- Single router + clients (baseline).
- 3-router cluster (peering/federation) with clients spread across routers.
- Optional 5-router for scale (later).

Deployment environments:
- Local: all processes on one host to validate harness.
- Distributed: 2–5 VMs/hosts for realistic network paths.

Metrics
Per run, collect:
- Throughput: sent/received msg/s and bytes/s (per role and total).
- Latency: p50/p90/p95/p99/max of end-to-end message latency.
- Reliability: loss rate (missed messages), duplicates.
- Resource: CPU%, memory, NIC throughput per host.

For queries additionally collect:
- Query latency: time-to-first-reply and time-to-last-reply (if multi-reply), per-reply latency distribution.
- QPS: requests issued vs completed; timeout rate; error rate.
- Result metrics: replies per query, total bytes per query, empty-result rate (misses).

Implementation notes:
- Timestamp each message at publish time; subscriber computes latency from embedded timestamp (account for clock skew if cross-host).
- Export CSV for easy post-processing; optional Prometheus endpoint for live scraping.

Failure and resilience checks
Add lightweight fault injections to representative scenarios:
- Router restart or rolling restart within the topology.
- Client churn: burst connect/disconnect.
- Network impairment via `tc netem`: latency/jitter/loss; optional bandwidth caps.
- Oversubscription: push far beyond capacity and observe behavior and recovery.
For queries also test:
- Partial responder availability (some queryables down) and requester behavior (timeouts, partial aggregation).
- Storage/queryable restart during load; check recovery and tail latency impact.

Harness design (no code yet)
A small CLI tool (to be implemented later) with two roles:
- Publisher role
  - Parameters: endpoint(s), topics, topic-count, publishers, payload-size, rate per pub, duration, reliability, QoS, key expressions.
  - Behavior: open-loop rate generation (with optional backoff) and timestamped payloads.
- Subscriber role
  - Parameters: endpoint(s), topics/expressions, subscribers, reliability, output CSV path, optional Prometheus port.
  - Behavior: receive, compute latency stats, count loss/dupes (sequence numbers), emit metrics.

Add two complementary roles for query testing:
- Requester role
  - Parameters: endpoint(s), key expression(s), rate (qps), concurrency (in-flight), timeout, aggregation window for multi-reply, duration, output CSV path.
  - Behavior: issue queries at target rate; record time-to-first/last reply, reply counts, bytes; track timeouts/errors.
- Queryable role
  - Parameters: key prefix(es) served, payload size, optional artificial processing delay/jitter, reliability, output CSV path.
  - Behavior: respond to queries matching served keys; optionally emit metrics on per-request handling time and bytes.

Operational features:
- Warm-up/cool-down trimming; periodic metric snapshots (e.g., 1s).
- Run ID tagging for files and logs.
- Exit summary printed and saved.

Orchestration (Docker-first, static topology)
We will use Docker Compose as the primary harness with a hardcoded, static topology. Auto-discovery/auto-peering is disabled.

Principles:
- No auto connecting: disable Zenoh scouting/auto-discovery and any automatic peering.
- Static topology: routers explicitly peer with configured TCP endpoints; clients connect to specific routers.
- Deterministic service names and ports: Compose service names become DNS entries (e.g., router1:7447).

Topologies in Compose:
- Single-router baseline: one router service (router1) plus client services (pub/sub/requester/queryable) attached to the same user-defined network.
- 3-router cluster (static peering): router1, router2, router3 with explicit peer lists:
  - Star: router2 <-> router1, router3 <-> router1 (simple hub-and-spoke).
  - Ring: router1 <-> router2, router2 <-> router3, router3 <-> router1 (resilient path diversity).

Discovery and peering settings (conceptual, to be encoded in config files later):
- Disable discovery/scouting: set the equivalent of `scouting.enabled = false` (or environment variable like `ZENOH_SCOUTING=0`).
- Routers: define `listeners = ["tcp/0.0.0.0:7447"]` and `peers = ["tcp/routerX:7447", ...]` per the chosen topology.
- Clients: connect using explicit endpoints, e.g., `tcp://router1:7447`.

Compose networking and isolation:
- One user-defined bridge network for the test (e.g., `zenoh-net`).
- Optional additional networks to simulate cross-AZ latency using tc/netem in specific containers.

Artifact plan (to be added later, no code now):
- `docker-compose.yml`: services for router1..N and client roles with resource limits and fixed ports.
- `config/router1.json5`, `config/router2.json5`, `config/router3.json5`: static configs with discovery disabled and static peers.
- `scripts/compose_up.sh`, `scripts/compose_down.sh`: convenience wrappers.

Basic scripts to include later:
- scripts/run_local_baseline.sh: 1 router, 1→1 scenarios sweep (Compose).
- scripts/run_fanout.sh: 1→N fanout sweeps (Compose; clients pinned to router1).
- scripts/run_cluster_3r.sh: 3 static-peered routers; clients distributed across routers.
- scripts/faults.sh: inject router restarts and tc netem impairments (Linux only, may require privileged containers).
- scripts/run_queries.sh: spawn requesters and multiple queryables; sweep selectivity, responders, and qps.

Run flow with Docker (conceptual)
1) Bring up routers only: `router1`, `router2`, `router3` with static configs; verify peers established via logs.
2) Start clients (publishers/subscribers/requesters/queryables) bound to specific routers via explicit endpoints.
3) Execute scenario sweep by restarting client containers with new parameters; keep routers running.
4) Collect CSV artifacts via bind mounts or `docker cp`.

Configuration and inputs
Common parameters (env vars or CLI flags):
- Connection: locator(s)/endpoints, transport (TCP/QUIC, as available), auth if needed.
- Workload: payload-size, topics, topic-count, pub/sub counts, rate per pub, duration, reliability/QoS.
- Metrics: CSV output dir, Prometheus port (optional), snapshot interval, warmup/cooldown percent.
- System: CPU pinning (optional), process niceness, GC/allocator tuning if relevant.

Data and reporting
Artifacts per run:
- CSV files: per-role timeseries (t, sent, recv, throughput, latency pXX, loss, dupe count).
- Summary JSON: scenario parameters + aggregated stats.
- Optional Prometheus scrape for live dashboards.

For queries add:
- CSV fields for time-to-first/last, replies-per-query, result-bytes, timeout/error counts.

Reporting (lightweight):
- A simple Python/R script or notebook (later) to create:
  - Latency CDFs and time series.
  - Throughput vs latency curves.
  - Resource overlays.

Methodology and quality
- Time sync: use chrony/NTP across hosts; otherwise estimate clock offset (publisher-sub handshake) and correct subscriber latency.
- Repeatability: 3 runs per point; report mean ± stddev.
- Trimming: discard first/last 10% of samples for warm-up/cool-down.
- Capacity finding: sweep rate upward until p99 latency grows sharply or loss appears; mark knee point.

Risks and mitigations
- CPU saturation of load generator on single host: distribute publishers/subscribers or reduce per-process overhead; pin CPUs if needed.
- Network limits: validate NIC capacity and avoid container NAT bottlenecks when measuring peak throughput.
- Skewed clocks: prefer same host for latency validation; use offset estimation for multi-host.

Minimal execution plan
Phase 1 – Local baseline
- Single router; 1→1; payloads 100 B/1 KB/10 KB; rates up to saturation.
- Verify metrics correctness and CSV outputs.

Phase 2 – Fanout and aggregation
- 1→N and N→1 with N in {10, 100}; identify knee points.

Phase 3 – 3-router cluster
- Repeat selected scenarios; compare to single-router.

Phase 4 – Faults
- Router restart and moderate netem impairments on selected scenarios.

Phase 5 – Query tests
- Single queryable: exact key vs wildcard; sweep qps and concurrency; measure time-to-first/last.
- Multiple queryables (3 and 10): wildcard queries; aggregate all replies within timeout window; measure partial-results behavior under faults.

Prerequisites
- Zenoh version to test (to be pinned).
- Docker and Docker Compose installed.
- Linux hosts with permission to run tc netem (optional) and install dependencies; for in-container netem, containers may need `CAP_NET_ADMIN`.
- Basic observability: node exporter (optional) or equivalent to capture CPU/mem/NIC.

Next steps
- Confirm Zenoh version, transport(s) to test, and topology sizes (1/3/5 routers).
- Implement the small CLI harness (publisher/subscriber/requester/queryable) with CSV output and basic stats.
- Add Docker Compose files and static router configs with auto-discovery disabled.
- Add simple scripts to run the minimal matrix and collect artifacts under Compose.
- Iterate based on first measurements and bottlenecks observed.

Questions to confirm:
- Do we prefer Prometheus/Grafana or CSV-only for the first iteration?
- Any hard SLOs or KPIs we must meet?
- Available host count and whether cross-host tests are required from the start?