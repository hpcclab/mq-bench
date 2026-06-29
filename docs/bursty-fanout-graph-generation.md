# Bursty Fan-Out Graph Generation

This document describes the graph-generation path for the bursty fan-out benchmark results, including the core files and the data flow from raw benchmark artifacts to the final plot images.

## Core Files

| File | Role |
|---|---|
| `scripts/orchestrate_fanout_bursty_load.sh` | Main end-to-end runner. It creates the result directories, runs each transport or MQTT broker, copies raw artifacts, calls the summarizer, writes the phase-rate note, and finally invokes the plotter. |
| `scripts/summarize_bursty_fanout.py` | Converts one raw run artifact directory into per-phase rows in `summary_by_phase.csv`. |
| `scripts/plot_bursty_fanout.py` | Main graph generator. It reads `summary_by_phase.csv`, builds or reuses `plot_points.csv`, and writes the plots under `plots/`. |
| `scripts/plot_results.py` | Shared plotting style helpers used by `plot_bursty_fanout.py`, including transport colors, markers, LaTeX sizing, and common visual constants. |
| `scripts/merge_bursty_fanout_results.py` | Optional helper for combining multiple `summary_by_phase.csv` files into one summary before plotting. |
| `results/<group>/raw_data/summary_by_phase.csv` | Primary phase summary consumed by the plotter. Example: `results/mqtt/raw_data/summary_by_phase.csv`. |
| `results/<group>/raw_data/plot_points.csv` | Cached readable time-series data used by the time-based plots. Example: `results/mqtt/raw_data/plot_points.csv`. |
| `results/<group>/plots/` | Final graph output directory. Example: `results/mqtt/plots/`. |

## Raw Inputs

Each benchmark run stores raw artifacts under:

```text
results/<group>/raw_data/<run_id>/fanout_singlesite/
```

The important raw files inside each run directory are:

| File pattern | Purpose |
|---|---|
| `sub_agg.csv` | Aggregate subscriber counters, delivery throughput, connection counts, and latency samples over time. |
| `pub_*.csv` | Per-publisher sent counters and publish rate over time. These establish the active publish timeline and sent totals. |
| `docker_stats.csv` | Broker/container CPU, memory, and network counters sampled during the run. |
| `EARLY_STOP.json` | Optional marker used when a broker fails or the orchestrator stops a run early. |

## End-To-End Process

1. `scripts/orchestrate_fanout_bursty_load.sh` prepares the benchmark directory:

```text
results/fanout_bursty_load_<timestamp>/
  raw_data/
  plots/
```

When appending to an existing grouped result, such as `results/mqtt`, the same structure is used:

```text
results/mqtt/
  raw_data/
  plots/
```

2. The orchestrator runs each selected transport or MQTT broker. For MQTT, broker-specific run IDs look like:

```text
fanout_bursty_<timestamp>_mqtt_artemis_p64_s1000_u10_bursty
```

3. The orchestrator copies the raw run artifacts into:

```text
results/<group>/raw_data/<run_id>/fanout_singlesite/
```

4. For each run, the orchestrator calls `scripts/summarize_bursty_fanout.py`. The summarizer reads `sub_agg.csv`, `pub_*.csv`, and `docker_stats.csv`, slices them by the configured rate profile, and appends one row per phase to:

```text
results/<group>/raw_data/summary_by_phase.csv
```

Important fields in `summary_by_phase.csv` include:

| Field | Meaning |
|---|---|
| `transport` | Transport or broker label, such as `mqtt_artemis`. |
| `phase`, `phase_start_s`, `phase_end_s` | Phase identity and phase boundaries. |
| `rate_per_pub`, `rate`, `delivery_rate` | Input rate per publisher, total publish rate, and expected fan-out delivery rate. |
| `sub_tps`, `pub_tps` | Observed subscriber and publisher throughput for the phase. |
| `p50_ms`, `p95_ms`, `p99_ms`, `avg_latency_ms` | Phase latency summaries. |
| `loss_pct` | Delivery deficit relative to expected fan-out. |
| `max_cpu_perc`, `avg_cpu_perc`, `max_mem_used_bytes` | Resource summaries from Docker stats. |
| `artifacts_dir` | Raw artifact directory used to rebuild time-series plot points. |

5. The orchestrator writes a phase-rate explanation file:

```text
results/<group>/phase-rate-summary.md
```

For appended runs, it may write a timestamped append note such as:

```text
results/<group>/phase-rate-summary-append-<timestamp>.md
```

6. The orchestrator calls `scripts/plot_bursty_fanout.py` with the summary CSV and output plot directory:

```bash
python3 scripts/plot_bursty_fanout.py \
  --summary results/mqtt/raw_data/summary_by_phase.csv \
  --out-dir results/mqtt/plots \
  --profile "<rate-profile>" \
  --latex
```

7. The plotter reads `summary_by_phase.csv`, resolves each row's `artifacts_dir`, and builds the time-series cache:

```text
results/<group>/raw_data/plot_points.csv
```

This cache contains normalized fields used by the line plots:

| Field | Meaning |
|---|---|
| `transport`, `run_id` | Series identity. |
| `time_s`, `phase`, `phase_time_s` | Time after warmup and phase-relative time. |
| `target_throughput_msg_s` | Expected fan-out target for the phase. |
| `delivery_throughput_msg_s` | Observed aggregate delivery throughput. |
| `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, `avg_latency_ms` | Time-series latency values. |
| `cpu_cores` | Docker CPU converted from percent into cores. |
| `memory_gb` | Broker memory usage in GiB. |
| `network_rx_gbps`, `network_tx_gbps` | Broker network throughput. |

8. The plotter writes final graphs to:

```text
results/<group>/plots/
```

The current bursty fan-out graph set includes:

| Plot | Output file |
|---|---|
| Delivery over time | `delivery_throughput_vs_time.png` |
| P99 latency over time | `p99_latency_vs_time.png` |
| P95 latency over time | `p95_latency_vs_time.png` |
| P50 latency over time | `p50_latency_vs_time.png` |
| Average latency over time | `avg_latency_vs_time.png` |
| CPU over time | `cpu_vs_time.png` |
| Memory over time | `memory_vs_time.png` |
| Network transmit over time | `network_tx_vs_time.png` |
| Phase latency whisker plot | `phase_latency_whisker.png` |
| Phase P99 latency | `phase_p99_latency.png` |
| Phase average latency | `phase_avg_latency.png` |
| Phase delivery throughput | `phase_delivery_throughput.png` |
| Phase CPU | `phase_cpu.png` |
| Phase memory | `phase_memory.png` |
| Standalone legend | `legend.png` |
| Plot gallery index | `README.md` |

With `--latex`, `plot_bursty_fanout.py` writes PDF versions and PNG siblings.

## Regenerating MQTT Graphs

To regenerate plots from the existing MQTT summary and raw artifacts:

```bash
python3 scripts/plot_bursty_fanout.py \
  --summary results/mqtt/raw_data/summary_by_phase.csv \
  --out-dir results/mqtt/plots \
  --profile "warmup:60:20,baseline:60:20,burst:60:50,recovery:60:20,burst:60:100,recovery:60:20,burst:60:200,recovery:60:20,burst:60:300,recovery:60:50,burst:60:500,recovery:60:50" \
  --latex \
  --rebuild-plot-points
```

Use `--rebuild-plot-points` when raw artifacts changed, result paths moved, append/rerun data was added, or the plot cache may be stale. Without that flag, the plotter reuses `plot_points.csv` when it considers the cache current.

## Optional Merge Flow

If multiple benchmark groups need to be plotted together, merge their phase summaries first:

```bash
python3 scripts/merge_bursty_fanout_results.py \
  results/mqtt \
  results/non-mqtt \
  --out results/combined/raw_data/summary_by_phase.csv
```

Then plot the merged summary:

```bash
python3 scripts/plot_bursty_fanout.py \
  --summary results/combined/raw_data/summary_by_phase.csv \
  --out-dir results/combined/plots \
  --rebuild-plot-points
```

## Mental Model

The phase summary CSV powers the phase bar and whisker plots. The raw artifact directories power the time-series plots through `plot_points.csv`. The plotter needs both: `summary_by_phase.csv` tells it what runs and phases exist, while each row's `artifacts_dir` points back to the raw per-second data needed for delivery, latency, CPU, memory, and network graphs.
