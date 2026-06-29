# Redis CPU Threading Regression

## Observation

In the `results/non-mqtt/raw_data/summary_by_phase.csv` bursty fan-out run, Redis appears to use more than one CPU core. Several Redis phases report broker CPU around four cores:

| Source | Value |
|---|---:|
| `summary_by_phase.csv` max Redis CPU | `401.84%` |
| Raw `docker_stats.csv` peak Redis CPU | `401.84%` |
| Raw samples above `300%` CPU | `692 / 725` |

Docker reports CPU as a percentage of one core. A value near `400%` means the Redis container used roughly four cores.

This conflicted with earlier experiments where Redis behaved like a mostly single-threaded broker and stayed close to one core.

## Root Cause

Redis itself was not behaving unexpectedly. The benchmark stack was starting Redis with explicit threaded I/O enabled.

In commit `59d1ceb5` (`Updates for bursty workload`, June 19, 2026), the Redis service in `docker-compose.yml` changed from:

```yaml
command: ["redis-server", "--save", "", "--appendonly", "no"]
```

to a longer command that included:

```yaml
- --io-threads
- "4"
- --io-threads-do-reads
- "yes"
```

Those settings allow Redis to use multiple I/O threads for socket reads and writes. Redis command execution is still centered on the main command path, but under high network fan-out the Redis process can consume multiple cores because the configured I/O threads are active.

## Why Older Runs Looked Different

Older saved Redis runs were produced before the threaded-I/O change, or with a Redis configuration equivalent to the simple command above. Those runs generally peaked around one core or below, matching the expected single-threaded Redis behavior for this benchmark.

The current `results/non-mqtt` run was generated after the June 19 configuration change, so it used Redis with four I/O threads enabled.

## Fix Applied

The Redis I/O-thread options were removed from `docker-compose.yml`:

```diff
-      - --io-threads
-      - "4"
-      - --io-threads-do-reads
-      - "yes"
```

The Redis service still keeps the other benchmark-oriented settings:

```yaml
- --save
- ""
- --appendonly
- "no"
- --tcp-backlog
- "65535"
- --timeout
- "0"
- --tcp-keepalive
- "60"
- --client-output-buffer-limit
- pubsub
- "0"
- "0"
- "0"
```

With the I/O-thread flags removed, future Redis benchmark runs should return to the earlier single-threaded behavior.

## How To Apply The Fix

If Redis is already running, recreate the container after changing `docker-compose.yml`:

```bash
docker compose up -d --force-recreate redis
```

For sequential benchmark scripts that bring services down and back up, the next run will pick up the fixed Redis command automatically.

## How To Verify

Check the running Redis command:

```bash
docker inspect redis --format '{{json .Config.Cmd}}'
```

The command should not include `--io-threads` or `--io-threads-do-reads`.

During a benchmark, confirm broker CPU from Docker stats:

```bash
docker stats --no-stream redis
```

For raw benchmark artifacts, inspect the Redis `docker_stats.csv` file and confirm CPU does not plateau near `400%`:

```bash
awk -F, 'NR>1 {gsub(/%/,"",$3); if ($3+0>m) m=$3+0} END {print m}' path/to/docker_stats.csv
```

## Notes

- `summary_by_phase.csv` copies CPU values from Docker stats; it did not create the multi-core reading on its own.
- `400%` in Docker stats means roughly four CPU cores, not four hundred percent of the whole host.
- Redis Pub/Sub fan-out can become network-I/O heavy, so enabling Redis threaded I/O materially changes the broker CPU profile.
- Results produced with the threaded-I/O Redis config should not be directly compared against older single-threaded Redis runs unless the configuration difference is called out.
