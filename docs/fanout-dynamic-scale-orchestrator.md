# Dynamic Fan-Out Scale Orchestrator

`scripts/orchestrate_fanout_dynamic_scale.sh` runs a controlled fan-out benchmark where the fan-out ratio stays fixed while the number of active publisher/topic groups changes over time.

Unlike the bursty fan-out script, which changes the per-publisher rate for all publisher groups, this script changes how many groups are active in each phase. All subscribers and publishers are started before the measured scale profile begins; inactive publisher groups stay connected and publish at `0 msg/s`.

## Topology

The script uses controlled fan-out:

```text
group_0: publisher_0 -> topic_0 -> SUBS_PER_PUB subscribers
group_1: publisher_1 -> topic_1 -> SUBS_PER_PUB subscribers
...
group_N: publisher_N -> topic_N -> SUBS_PER_PUB subscribers
```

The total number of groups is:

```text
NUM_GROUPS = SUBS / SUBS_PER_PUB
```

For example, `--subs 2000 --subs-per-pub 100` creates `20` publisher/topic groups and `100` subscribers per topic.

Each phase chooses how many of those groups are active. If a phase says `active_groups=5`, groups `0..4` publish at `GROUP_RATE`; the remaining groups stay connected at zero rate.

## Rate Model

`--group-rate` is the publish rate for each active publisher group.

For every phase:

```text
active_publish_rate = active_groups * group_rate
delivery_target = active_groups * group_rate * subs_per_pub
```

Example:

```text
--subs 2000
--subs-per-pub 100
--group-rate 1000
--scale-profile "baseline:60:1,burst:120:10,recovery:90:1"
```

This creates `20` total groups. During `burst`, `10` groups are active:

```text
active_publish_rate = 10 * 1000 = 10,000 msg/s
delivery_target = 10 * 1000 * 100 = 1,000,000 msg/s
```

## Scale Profile

The scale profile format is:

```text
phase_name:duration_seconds:active_groups,...
```

Default:

```text
baseline:60:1,burst:120:5,recovery:90:1,burst:120:10,recovery:90:1,burst:120:20,recovery:120:1
```

Rules:

- `duration_seconds` must be a positive integer.
- `active_groups` must be a positive integer.
- `max(active_groups)` cannot exceed `SUBS / SUBS_PER_PUB`.
- Repeated `burst` and `recovery` phases are relabeled in generated artifacts as `B1`, `R1`, `B2`, `R2`, and so on.

Inactive groups are still represented, but only inside the generated per-group rate profiles. The user-facing scale profile cannot use `0` active groups.

## Publisher Prestart

`PUB_PRESTART_SECS` defaults to `5`.

The script prepends a zero-rate warmup phase to every per-group publisher profile:

```text
warmup:PUB_PRESTART_SECS:0
```

This lets publishers connect before the real scale phases begin. The script then adds `PUB_PRESTART_SECS` to the total `DURATION` passed to `run_fanout.sh`.

Set it explicitly when needed:

```bash
PUB_PRESTART_SECS=10 scripts/orchestrate_fanout_dynamic_scale.sh ...
```

## Subscriber Ramp-Up

`SUB_RAMP_UP_SECS` controls how long `run_fanout.sh` waits after starting subscribers and before starting publishers.

If unset:

- `SUBS >= 2000` uses `60` seconds.
- Smaller runs use `0` seconds.

Override it with:

```bash
SUB_RAMP_UP_SECS=30 scripts/orchestrate_fanout_dynamic_scale.sh ...
```

## Common Usage

Run one remote Zenoh/NATS comparison, starting each broker sequentially:

```bash
SUB_RAMP_UP_SECS=60 bash scripts/orchestrate_fanout_dynamic_scale.sh \
  --host 192.168.0.245 \
  --transports "zenoh nats" \
  --subs 2000 \
  --subs-per-pub 100 \
  --sub-procs-per-topic 10 \
  --ssh-target ubuntu@192.168.0.245 \
  --remote-dir /home/ubuntu/mq-bench \
  --payload 128B \
  --group-rate 1000 \
  --sequential \
  --scale-profile "baseline:60:1,burst:120:5,recovery:90:1,burst:120:10,recovery:90:1,burst:120:20,recovery:120:1"
```

Dry-run the resolved commands:

```bash
DRY_RUN=1 bash scripts/orchestrate_fanout_dynamic_scale.sh \
  --transports "zenoh" \
  --subs 1000 \
  --subs-per-pub 100 \
  --group-rate 500 \
  --scale-profile "baseline:30:1,burst:30:5,recovery:30:1"
```

## Options

| Option | Default | Meaning |
|---|---:|---|
| `--host` | empty/local | Broker host used by clients and summaries. |
| `--transports` | `zenoh redis nats rabbitmq mqtt` | Space-separated transport list. |
| `--subs` | `2000` | Total subscribers. Must be divisible by `--subs-per-pub`. |
| `--subs-per-pub` | `100` | Subscribers per publisher/topic group. |
| `--sub-procs-per-topic` | `1` | Split each topic's subscribers across this many client processes. Useful when one subscriber process is the bottleneck. |
| `--payload` | `1024` | Payload size. Accepts raw bytes, `B`, `KB`, or `MB` suffixes. |
| `--group-rate` | `1000` | Message rate per active publisher group. |
| `--scale-profile` | see above | Phase list in `name:duration:active_groups` format. |
| `--snapshot` | `1` | Sampling interval passed to the benchmark and Docker stats monitor. |
| `--sequential` | off | Bring up only the selected broker service for each transport, then tear it down after the run. |
| `--ssh-target` | empty | Remote host for Docker Compose control and remote stats collection. |
| `--remote-dir` | `~/mq-bench` | Remote repo directory used with `--ssh-target`. |
| `--interval-sec`, `--cooldown-sec` | `120` | Cooldown between transport runs. |
| `--dry-run` | off | Print commands without running them. |

Environment knobs:

| Variable | Default | Meaning |
|---|---:|---|
| `PUB_PRESTART_SECS` | `5` | Zero-rate publisher prestart added before the scale profile. |
| `SUB_RAMP_UP_SECS` | auto | Subscriber startup wait before publishers begin. |
| `PLOT_BUCKET_SECONDS` | `1` | Bucket size for time-series plot points. |
| `INTERVAL_SEC` | `120` | Same as `--interval-sec` unless overridden by CLI. |
| `DRY_RUN` | `0` | Same as `--dry-run`. |

## Transport Mapping

The script maps transports to default services and ports:

| Transport | Compose service | Port |
|---|---|---:|
| `zenoh` | `router1` | `7447` |
| `redis` | `redis` | `6379` |
| `nats` | `nats` | `4222` |
| `rabbitmq` | `rabbitmq` | `5672` |
| `mqtt` | `mosquitto` | `1883` |

When `--sequential` is set, each mapped service is started with `docker compose up -d <service>`, checked with `wait_for_port`, then stopped with `docker compose down`.

## What It Generates

Each run creates:

```text
results/fanout_dynamic_scale_<timestamp>/
  phase-rate-summary.md
  raw_data/
    summary_by_phase.csv
    summary_rate_profile.txt
    group_profiles/
      group_0.profile
      group_1.profile
      ...
    <run_id>/fanout_singlesite/
      sub_agg.csv
      pub_agg.csv
      docker_stats.csv
      sub_*.csv
      pub_*.csv
      sub_*.log
      pub_*.log
  plots/
    ...
```

The most important files are:

- `phase-rate-summary.md`: human-readable phase table with active groups and target delivery rates.
- `raw_data/group_profiles/*.profile`: generated per-group publisher profiles. Active groups get `GROUP_RATE`; inactive groups get `0`.
- `raw_data/summary_by_phase.csv`: one row per transport and phase, including throughput, latency, loss, CPU, memory, and network stats.
- `raw_data/<run_id>/fanout_singlesite/docker_stats.csv`: broker/container utilization samples.
- `plots/`: generated time-series and summary plots from `plot_bursty_fanout.py`.

## How Summaries Work

The per-group profiles drive the real publishers, but the summarizer needs one profile string for target-rate plots. The script writes `summary_rate_profile.txt` using the average rate per group:

```text
average_rate_per_group = active_groups * group_rate / total_groups
```

Then `summarize_bursty_fanout.py` receives:

```text
pubs = total_groups
subs_per_pub = SUBS_PER_PUB
profile = summary_rate_profile
```

This preserves the correct total target:

```text
average_rate_per_group * total_groups * subs_per_pub
  = active_groups * group_rate * subs_per_pub
```

## Reading Results

Use `summary_by_phase.csv` to compare phases and transports:

- `delivery_rate`: target fan-out delivery rate for the phase.
- `sub_tps`: observed subscriber-side delivered throughput.
- `pub_tps`: observed publisher-side send throughput.
- `loss_pct`: difference between sent fanout volume and received fanout volume.
- `p50_ms`, `p95_ms`, `p99_ms`: latency percentiles for the phase.
- `max_cpu_perc`, `avg_cpu_perc`: broker/container CPU use.
- `max_net_tx_bps`, `avg_net_tx_bps`: broker/container egress pressure.

For saturation diagnosis:

- If `sub_tps` flattens while `max_net_tx_bps` approaches link capacity, the run is likely network egress constrained.
- If `sub_tps` flattens while CPU is pinned and network is below link capacity, the run is likely broker CPU or message-rate constrained.
- If latency grows across a burst and recovers only after the burst ends, the broker or network built a queue/backlog.

## Implementation Notes

The orchestrator delegates client execution to `scripts/run_fanout.sh` with these key environment variables:

```text
CONTROLLED_FANOUT=1
PUBS=<NUM_GROUPS>
SUBS=<SUBS>
SUBS_PER_PUB=<SUBS_PER_PUB>
SUB_PROCS_PER_TOPIC=<SUB_PROCS_PER_TOPIC>
RATE_PROFILE_DIR=<generated group_profiles directory>
PAYLOAD=<payload bytes>
DURATION=<sum(scale profile durations) + PUB_PRESTART_SECS>
SNAPSHOT=<snapshot seconds>
```

Transport-specific connection variables are added per run, such as `ENDPOINT_SUB`/`ENDPOINT_PUB` for Zenoh, `NATS_HOST` for NATS, and `REDIS_URL` for Redis.

Remote broker stats are collected with `scripts/collect_remote_docker_stats.sh` when `--host` points at a remote broker. Local stats are collected by `run_fanout.sh` through `MONITOR_CONTAINERS`.
