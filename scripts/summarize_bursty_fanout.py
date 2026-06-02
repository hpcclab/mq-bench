#!/usr/bin/env python3
"""Create per-phase summaries for bursty fan-out runs."""
import argparse
import csv
import glob
import math
import os
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

HEADER = [
    "transport", "host", "port", "payload", "subs", "pubs", "phase",
    "phase_start_s", "phase_end_s", "rate_per_pub", "rate", "delivery_rate",
    "run_id", "sub_tps", "p50_ms", "p95_ms", "p99_ms", "avg_latency_ms", "pub_tps", "sent",
    "recv", "errors", "loss_pct", "artifacts_dir", "max_cpu_perc",
    "max_mem_perc", "max_mem_used_bytes", "avg_cpu_perc", "avg_mem_perc",
    "avg_mem_used_bytes", "max_net_rx_bps", "max_net_tx_bps", "avg_net_rx_bps",
    "avg_net_tx_bps",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--transport", required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--port", default="")
    p.add_argument("--payload", required=True, type=int)
    p.add_argument("--subs", required=True, type=int)
    p.add_argument("--pubs", required=True, type=int)
    p.add_argument("--profile", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--artifacts-dir", required=True)
    return p.parse_args()


def parse_profile(spec: str) -> List[Dict[str, float]]:
    phases = []
    cursor = 0.0
    for raw in spec.split(','):
        raw = raw.strip()
        if not raw:
            continue
        parts = [p.strip() for p in raw.split(':')]
        if len(parts) != 3:
            raise ValueError(f"invalid phase '{raw}', expected name:duration:rate")
        name, duration_s, rate = parts
        duration = float(duration_s)
        rate_f = float(rate)
        if not name or duration <= 0 or rate_f <= 0:
            raise ValueError(f"invalid phase '{raw}'")
        phases.append({"name": name, "start": cursor, "end": cursor + duration, "rate": rate_f})
        cursor += duration
    if not phases:
        raise ValueError("profile must contain at least one phase")
    return phases


def read_rows(path: str) -> List[Dict[str, str]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))


def fnum(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        val = row.get(key, "")
        if val is None or val == "":
            return default
        return float(str(val).replace('%', ''))
    except Exception:
        return default


def timestamp(row: Dict[str, str]) -> float:
    return fnum(row, "timestamp", fnum(row, "ts", 0.0))


def counter_delta(rows: List[Dict[str, str]], field: str, start_abs: float, end_abs: float) -> float:
    rows = sorted(rows, key=timestamp)
    before_val = 0.0
    end_val = None
    for row in rows:
        ts = timestamp(row)
        val = fnum(row, field, 0.0)
        if ts < start_abs:
            before_val = val
        if ts <= end_abs:
            end_val = val
        else:
            break
    if end_val is None:
        return 0.0
    return max(0.0, end_val - before_val)


def weighted_latency_ms(rows: List[Dict[str, str]], start_abs: float, end_abs: float, metric: str) -> str:
    interval_key = f"interval_latency_ns_{metric}"
    cumulative_key = f"latency_ns_{metric}"
    total_weight = 0.0
    total_value = 0.0
    fallback_values = []
    for row in rows:
        ts = timestamp(row)
        if not (start_abs < ts <= end_abs):
            continue
        if interval_key in row and row.get(interval_key, "") != "":
            samples = fnum(row, "interval_latency_sample_count", 0.0)
            value = fnum(row, interval_key, 0.0)
            if samples > 0 and value > 0:
                total_weight += samples
                total_value += value * samples
        else:
            value = fnum(row, cumulative_key, 0.0)
            if value > 0:
                fallback_values.append(value)
    if total_weight > 0:
        return f"{(total_value / total_weight) / 1_000_000.0:.3f}"
    if fallback_values:
        return f"{(sum(fallback_values) / len(fallback_values)) / 1_000_000.0:.3f}"
    return ""



def mean_latency_ms(rows: List[Dict[str, str]], start_abs: float, end_abs: float) -> str:
    total_weight = 0.0
    total_value = 0.0
    has_interval_mean = any("interval_latency_ns_mean" in row for row in rows)
    if has_interval_mean:
        for row in rows:
            ts = timestamp(row)
            if not (start_abs < ts <= end_abs):
                continue
            samples = fnum(row, "interval_latency_sample_count", 0.0)
            mean = fnum(row, "interval_latency_ns_mean", 0.0)
            if samples > 0 and mean > 0:
                total_weight += samples
                total_value += mean * samples
        if total_weight > 0:
            return f"{(total_value / total_weight) / 1_000_000.0:.3f}"

    rows = sorted(rows, key=timestamp)
    before_count = 0.0
    before_mean = 0.0
    end_count = None
    end_mean = 0.0
    for row in rows:
        ts = timestamp(row)
        count = fnum(row, "latency_sample_count", 0.0)
        mean = fnum(row, "latency_ns_mean", 0.0)
        if ts < start_abs:
            before_count = count
            before_mean = mean
        if ts <= end_abs:
            end_count = count
            end_mean = mean
        else:
            break
    if end_count is None:
        return ""
    samples = end_count - before_count
    if samples <= 0:
        return ""
    total_ns = (end_mean * end_count) - (before_mean * before_count)
    if total_ns <= 0:
        return ""
    return f"{(total_ns / samples) / 1_000_000.0:.3f}"

def parse_bytes_token(raw: str) -> float:
    raw = (raw or "").strip()
    if not raw or raw == "-":
        return 0.0
    if "/" in raw:
        raw = raw.split("/", 1)[0].strip()
    m = re.match(r"^([0-9.]+)\s*([A-Za-z]*)$", raw)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2)
    factors = {
        "": 1, "B": 1,
        "kB": 1000, "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3, "TB": 1000 ** 4,
        "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "TiB": 1024 ** 4,
    }
    return val * factors.get(unit, 1)


def stats_value(row: Dict[str, str], *keys: str) -> float:
    for key in keys:
        if key in row and row.get(key, "") != "":
            if key == "mem_usage":
                return parse_bytes_token(row[key])
            return fnum(row, key, 0.0)
    return 0.0


def aggregate_stats(rows: List[Dict[str, str]], start_abs: float, end_abs: float) -> Dict[str, str]:
    selected = [r for r in rows if start_abs <= timestamp(r) <= end_abs]
    if not selected:
        return {k: "" for k in ("max_cpu", "max_mem", "max_mem_used", "avg_cpu", "avg_mem", "avg_mem_used", "max_rx", "max_tx", "avg_rx", "avg_tx")}

    cpus = [stats_value(r, "cpu_perc_num", "cpu_perc") for r in selected]
    mems = [stats_value(r, "mem_perc_calc", "mem_perc") for r in selected]
    mem_used = [stats_value(r, "mem_used_b", "mem_usage") for r in selected]

    by_ts: Dict[float, List[float]] = defaultdict(lambda: [0.0, 0.0])
    for row in selected:
        ts = timestamp(row)
        by_ts[ts][0] += stats_value(row, "net_rx_b")
        by_ts[ts][1] += stats_value(row, "net_tx_b")

    rx_rates = []
    tx_rates = []
    prev_ts = None
    prev_rx = prev_tx = 0.0
    for ts in sorted(by_ts):
        rx, tx = by_ts[ts]
        if prev_ts is not None and ts > prev_ts:
            drx = rx - prev_rx
            dtx = tx - prev_tx
            if drx >= 0 and dtx >= 0:
                dt = ts - prev_ts
                rx_rates.append((drx * 8.0) / dt)
                tx_rates.append((dtx * 8.0) / dt)
        prev_ts, prev_rx, prev_tx = ts, rx, tx

    def avg(vals: Iterable[float]) -> float:
        vals = list(vals)
        return sum(vals) / len(vals) if vals else 0.0

    return {
        "max_cpu": f"{max(cpus):.6f}" if cpus else "",
        "max_mem": f"{max(mems):.6f}" if mems else "",
        "max_mem_used": f"{max(mem_used):.0f}" if mem_used else "",
        "avg_cpu": f"{avg(cpus):.6f}" if cpus else "",
        "avg_mem": f"{avg(mems):.6f}" if mems else "",
        "avg_mem_used": f"{avg(mem_used):.0f}" if mem_used else "",
        "max_rx": f"{max(rx_rates):.6f}" if rx_rates else "",
        "max_tx": f"{max(tx_rates):.6f}" if tx_rates else "",
        "avg_rx": f"{avg(rx_rates):.6f}" if rx_rates else "",
        "avg_tx": f"{avg(tx_rates):.6f}" if tx_rates else "",
    }


def fmt_float(value: float) -> str:
    if not math.isfinite(value):
        return ""
    return f"{value:.2f}"


def main() -> int:
    args = parse_args()
    phases = parse_profile(args.profile)
    art_dir = args.artifacts_dir
    sub_rows = read_rows(os.path.join(art_dir, "sub_agg.csv"))
    pub_files = sorted(glob.glob(os.path.join(art_dir, "pub_*.csv")))
    pub_rows_by_file = [read_rows(p) for p in pub_files if not p.endswith("pub_agg.csv")]
    stats_rows = read_rows(os.path.join(art_dir, "docker_stats.csv"))

    all_pub_rows = [row for rows in pub_rows_by_file for row in rows]
    start_candidates = [timestamp(r) for r in all_pub_rows if fnum(r, "sent_count", 0.0) > 0]
    if not start_candidates:
        start_candidates = [timestamp(r) for r in sub_rows if timestamp(r) > 0]
    if not start_candidates:
        raise SystemExit(f"no timestamped rows found in {art_dir}")
    run_start = min(start_candidates)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    write_header = not os.path.exists(args.out) or os.path.getsize(args.out) == 0
    with open(args.out, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        if write_header:
            writer.writeheader()
        for phase in phases:
            start_abs = run_start + phase["start"]
            end_abs = run_start + phase["end"]
            duration = max(phase["end"] - phase["start"], 1.0)

            sent = sum(counter_delta(rows, "sent_count", start_abs, end_abs) for rows in pub_rows_by_file)
            pub_errors = sum(counter_delta(rows, "error_count", start_abs, end_abs) for rows in pub_rows_by_file)
            recv = counter_delta(sub_rows, "received_count", start_abs, end_abs)
            sub_errors = counter_delta(sub_rows, "error_count", start_abs, end_abs)
            expected_recv = sent * args.subs
            loss_pct = ((expected_recv - recv) / expected_recv * 100.0) if expected_recv > 0 else 0.0
            stats = aggregate_stats(stats_rows, start_abs, end_abs)

            row = {
                "transport": args.transport,
                "host": args.host,
                "port": args.port,
                "payload": args.payload,
                "subs": args.subs,
                "pubs": args.pubs,
                "phase": phase["name"],
                "phase_start_s": f"{phase['start']:.0f}",
                "phase_end_s": f"{phase['end']:.0f}",
                "rate_per_pub": f"{phase['rate']:.2f}".rstrip('0').rstrip('.'),
                "rate": f"{phase['rate'] * args.pubs:.2f}".rstrip('0').rstrip('.'),
                "delivery_rate": f"{phase['rate'] * args.pubs * args.subs:.2f}".rstrip('0').rstrip('.'),
                "run_id": args.run_id,
                "sub_tps": fmt_float(recv / duration),
                "p50_ms": weighted_latency_ms(sub_rows, start_abs, end_abs, "p50"),
                "p95_ms": weighted_latency_ms(sub_rows, start_abs, end_abs, "p95"),
                "p99_ms": weighted_latency_ms(sub_rows, start_abs, end_abs, "p99"),
                "avg_latency_ms": mean_latency_ms(sub_rows, start_abs, end_abs),
                "pub_tps": fmt_float(sent / duration),
                "sent": f"{sent:.0f}",
                "recv": f"{recv:.0f}",
                "errors": f"{pub_errors + sub_errors:.0f}",
                "loss_pct": f"{loss_pct:.2f}",
                "artifacts_dir": art_dir,
                "max_cpu_perc": stats["max_cpu"],
                "max_mem_perc": stats["max_mem"],
                "max_mem_used_bytes": stats["max_mem_used"],
                "avg_cpu_perc": stats["avg_cpu"],
                "avg_mem_perc": stats["avg_mem"],
                "avg_mem_used_bytes": stats["avg_mem_used"],
                "max_net_rx_bps": stats["max_rx"],
                "max_net_tx_bps": stats["max_tx"],
                "avg_net_rx_bps": stats["avg_rx"],
                "avg_net_tx_bps": stats["avg_tx"],
            }
            writer.writerow(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
