# Bursty Fan-Out Experiment Plan

## Summary

Implement Experiment 3 from the journal-extension slides as a first-class bursty fan-out workload:

- Topology: `P publishers -> S subscribers`
- Topic layout: all publishers publish to one shared topic, all subscribers subscribe to that topic
- Subscriber count: `S = 1000`
- Publisher rule: `P = max(1, ceil(S / 100)) = 10`
- Payload: `1 KB`
- QoS: `0`
- Goal: measure overload absorption and recovery under a controlled bursty traffic profile

The repo needs changes. The current fan-out scripts and Rust publisher support only one constant publish rate per run. A bursty workload can be approximated by running separate constant-rate phases, but that would reconnect publishers between phases and hide the continuity needed to study recovery. The better implementation is a continuous publisher rate profile inside `mq-bench`.

## Traffic Profile

Use the slide profile exactly:

| Phase | Duration | Rate per publisher | Publish rate with 10 publishers | Delivery rate with 1000 subscribers |
|---|---:|---:|---:|---:|
| warmup | 60 s | 10 msg/s/pub | 100 msg/s | 100,000 deliveries/s |
| baseline | 60 s | 10 msg/s/pub | 100 msg/s | 100,000 deliveries/s |
| burst | 20 s | 30 msg/s/pub | 300 msg/s | 300,000 deliveries/s |
| elevated | 60 s | 15 msg/s/pub | 150 msg/s | 150,000 deliveries/s |
| recovery | 60 s | 10 msg/s/pub | 100 msg/s | 100,000 deliveries/s |

Interpretation:

- `warmup` gives the broker and clients time to settle.
- `baseline` captures normal fan-out behavior before overload.
- `burst` applies transient pressure.
- `elevated` shows whether the broker stabilizes after the spike.
- `recovery` shows whether latency, throughput, CPU, memory, and bandwidth return near baseline.

## Implementation Changes

### Rust Publisher Rate Profile

Add a publisher CLI option:

```text
--rate-profile "warmup:60:10,baseline:60:10,burst:60:30,elevated:60:15,recovery:60:10"
```

Behavior:

- Format: comma-separated `phase_name:duration_secs:rate_per_publisher`.
- If `--rate-profile` is present, it overrides `--rate`.
- Total publisher duration is the sum of all phase durations.
- Existing constant-rate behavior remains unchanged when `--rate-profile` is absent.
- Publishers remain connected for the full profile.
- The publisher switches rate controllers at phase boundaries.

Suggested code shape:

- Add `RatePhase` and `RateProfile` types near the existing rate-control code.
- Add parser and validation for the profile string.
- Add `rate_profile: Option<RateProfile>` to `PublisherConfig`.
- Extend `src/main.rs` so the `pub` subcommand accepts `--rate-profile`.
- In `src/roles/publisher.rs`, choose the current phase by elapsed time and reset the `RateController` whenever the phase changes.

Validation rules:

- Phase names must be non-empty.
- Durations must be positive.
- Rates must be positive.
- Empty profiles should fail with a clear CLI error.

### Bursty Fan-Out Orchestration

Add a new script:

```text
scripts/orchestrate_fanout_bursty_load.sh
```

Use the steady fan-out orchestrator as the base, but change the defaults:

```bash
SUBS=1000
SUBS_PER_PUB=100
PUBLISHERS=10
PAYLOAD_TOKEN="1024"
QOS=0
SNAPSHOT=1
RUN_ID_PREFIX="fanout_bursty"
RATE_PROFILE="warmup:60:10,baseline:60:10,burst:60:30,elevated:60:15,recovery:60:10"
```

Output layout:

```text
results/fanout_bursty_load_<timestamp>/
  raw_data/
    summary_by_phase.csv
  plots/
    README.md
```

Artifacts should continue to live under:

```text
artifacts/<run_id>/fanout_singlesite/
  sub_agg.csv
  pub_agg.csv
  docker_stats.csv
  sub.log
  pub_*.log
```

The script should retain support for:

- `--transports`
- `--host`
- `--ssh-target`
- `--sequential`
- `--mqtt-brokers`
- `--amqp-brokers`
- `--payload`
- `--snapshot`
- `--dry-run`

### Run Script Changes

Update `scripts/run_fanout.sh` so it can pass the profile to `mq-bench pub`.

Add environment variable support:

```bash
RATE_PROFILE="${RATE_PROFILE:-}"
```

When `RATE_PROFILE` is non-empty:

- Pass `--rate-profile "${RATE_PROFILE}"` to publisher commands.
- Do not pass `--rate`.
- Derive `DURATION` from the profile if not explicitly provided, or let the Rust CLI derive it.

When `RATE_PROFILE` is empty:

- Keep the existing constant-rate behavior exactly as it is.

### Phase Summary CSV

Add burst-specific summary extraction that slices raw CSV rows by phase time windows.

Create:

```text
summary_by_phase.csv
```

Columns:

```text
transport,host,port,payload,subs,pubs,phase,phase_start_s,phase_end_s,rate_per_pub,rate,delivery_rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,loss_pct,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,avg_cpu_perc,avg_mem_perc,avg_mem_used_bytes,max_net_rx_bps,max_net_tx_bps,avg_net_rx_bps,avg_net_tx_bps
```

Notes:

- Keep `warmup` in raw data and plots.
- Exclude `warmup` from paper-facing aggregate comparisons unless explicitly discussed.
- Compute phase boundaries relative to the first publisher/subscriber sample timestamp for each run.
- For fan-out loss, expected delivery count should be `sent * subs`.

### Plotting

Add a burst-specific plotting script rather than overloading `plot_results.py`:

```text
scripts/plot_bursty_fanout.py
```

Inputs:

```bash
python3 scripts/plot_bursty_fanout.py \
  --summary results/fanout_bursty_load_<timestamp>/raw_data/summary_by_phase.csv \
  --out-dir results/fanout_bursty_load_<timestamp>/plots
```

Outputs:

- Time-series delivery throughput vs time, with vertical phase boundaries.
- Time-series p50/p95/p99 latency vs time.
- Time-series CPU utilization vs time.
- Time-series memory utilization vs time.
- Time-series network bandwidth vs time.
- Per-phase p99 latency comparison by broker.
- Per-phase delivered throughput comparison by broker.
- `README.md` gallery embedding generated figures.

## Test Plan

- Unit-test rate-profile parsing:
  - valid profile parses names, durations, rates, and total duration.
  - malformed entries fail clearly.
  - profile overrides constant rate only when present.
- Unit-test phase selection:
  - elapsed time maps correctly to `warmup`, `baseline`, `burst`, `elevated`, and `recovery`.
  - exact phase boundaries choose the new phase.
- Smoke-test the CLI:
  - run a short profile such as `warmup:1:10,burst:1:30,recovery:1:10`.
  - confirm publisher output rate changes across phases.
- Dry-run orchestration:
  - `scripts/orchestrate_fanout_bursty_load.sh --dry-run --transports "zenoh"`
  - confirm the generated command includes `--rate-profile`.
- Plot validation:
  - run plotting against a small generated burst result.
  - confirm `summary_by_phase.csv`, time-series plots, per-phase plots, and gallery are produced.

## Assumptions

- The journal experiment should use the PPTX profile exactly.
- Bursty fan-out should be continuous within one run, with no publisher reconnects between phases.
- Existing steady fan-out and fan-in experiments must remain backward compatible.
- The primary paper-facing metrics are delivery throughput, p95/p99 latency, CPU utilization, memory footprint, and network bandwidth over time.
