# Zenoh CPU Plateau Around 3 Cores

## Observation

In the Zenoh bursty fan-out run, router CPU appears to flatten around 3 CPU cores. The time-series data does go slightly above 3.1 cores, with a peak around 3.18 cores, but it does not continue scaling with higher offered load.

At the same time, Zenoh starts missing the throughput target in the higher burst phases:

| Phase | Delivery target | Observed delivery | Loss |
|---|---:|---:|---:|
| burst 3k/pub | 3.0M msg/s | ~3.00M msg/s | ~0% |
| burst 4k/pub | 4.0M msg/s | ~3.60M msg/s | ~9.95% |
| burst 5k/pub | 5.0M msg/s | ~3.96M msg/s | ~20.74% |

The publishers report no errors or reconnects, so the publish side is still sending as programmed. The bottleneck appears downstream: router receive, router fan-out, router transmit, subscriber receive, or network egress.

## Why It Happens

This does not look like a Docker CPU cap.

`docker-compose.yml` defines `router1` without `cpus`, `cpuset`, or CPU quota settings. The router service only sets:

```yaml
environment:
  RUST_LOG: info
```

The more likely cause is Zenoh's own runtime and transport threading defaults.

Zenoh 1.7.0 uses separate internal runtimes. The defaults are small:

```text
app: 1 worker thread
acc: 1 worker thread
tx:  1 worker thread
rx:  2 worker threads
net: 1 worker thread
```

These are controlled by the `ZENOH_RUNTIME` environment variable and only take effect at process startup.

Zenoh also has a separate transport TX thread count:

```text
transport/link/tx/threads = 1 + ((num_cpus - 1) / 4)
```

On a 4-vCPU host, that default is `1`.

Your `config/router1.json5` increases TCP batching and RX buffer size, but it does not increase TX threads:

```json5
link: {
  tx: {
    batch_size: 65535,
  },
  rx: {
    buffer_size: 131072,
  }
}
```

So the single-router TCP fan-out path is probably saturating a limited set of Zenoh runtime/TX workers. That matches the observed shape: throughput stops scaling, latency explodes, loss appears, and router CPU stays near 3 cores rather than consuming all available CPU.

## How To Confirm

Run the same Zenoh experiment with higher router runtime and TX parallelism.

In `docker-compose.yml`:

```yaml
router1:
  environment:
    RUST_LOG: info
    ZENOH_RUNTIME: '(rx: (worker_threads: 4), tx: (worker_threads: 4), net: (worker_threads: 2), app: (worker_threads: 2), acc: (worker_threads: 1))'
```

In `config/router1.json5`:

```json5
transport: {
  unicast: {
    max_sessions: 100000,
    accept_pending: 4096,
    accept_timeout: 30000,
  },
  link: {
    tx: {
      batch_size: 65535,
      threads: 4,
    },
    rx: {
      buffer_size: 131072,
    }
  }
}
```

If CPU rises above ~3.2 cores and the 4M/5M delivered-message phases improve, that confirms the plateau is caused by Zenoh runtime/TX parallelism rather than Docker or the benchmark publisher rate limiter.

## Notes

- The CPU metric in `plot_points.csv` is broker-container CPU only, not total host CPU.
- For Zenoh, the monitored container is `router1`.
- Publishers and subscribers are separate `mq-bench` processes and are not included in the plotted broker CPU.
- The exact copied `docker_stats.csv` under `results/zenoh` appears to be in remote collector format and does not fully reproduce the plotted CPU cores. Treat `summary_by_phase.csv` and `plot_points.csv` as the authoritative plotted outputs for this run.
