#!/usr/bin/env python3
"""
Backfill or fix utilization columns in a summary CSV by recomputing per-run
max CPU%, max Memory% and max Memory used bytes from docker_stats.csv in each
run's artifacts directory.

Usage:
  scripts/backfill_utilization.py --summary path/to/summary.csv [--out path]

By default edits the file in place; use --out to write to a new file.
"""
import argparse
import csv
import math
import os
from typing import Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True)
    p.add_argument("--out", default=None, help="Optional output path (default: overwrite input)")
    return p.parse_args()


def to_bytes(token: str) -> float:
    token = token.strip()
    if not token:
        return 0.0
    # Expect forms like "3.77MiB", "187.4GiB", "573GB"
    num = ""
    unit = ""
    for ch in token:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            unit += ch
    try:
        val = float(num) if num else 0.0
    except ValueError:
        return 0.0
    mult = 1.0
    if unit == "B":
        mult = 1.0
    elif unit in ("kB", "KB"):
        mult = 1000.0
    elif unit == "MB":
        mult = 1000.0 ** 2
    elif unit == "GB":
        mult = 1000.0 ** 3
    elif unit == "TB":
        mult = 1000.0 ** 4
    elif unit == "KiB":
        mult = 1024.0
    elif unit == "MiB":
        mult = 1024.0 ** 2
    elif unit == "GiB":
        mult = 1024.0 ** 3
    elif unit == "TiB":
        mult = 1024.0 ** 4
    return val * mult


def compute_from_stats(stats_csv: str) -> Tuple[float, float, float]:
    if not os.path.exists(stats_csv):
        return (math.nan, math.nan, math.nan)
    max_cpu = 0.0
    max_mem_perc = 0.0
    max_used = 0.0
    with open(stats_csv, newline="") as f:
        rd = csv.reader(f)
        for row in rd:
            # Expect: ts,container,name,cpu_perc,mem_usage,mem_perc,net_io,block_io,pids
            if not row or row[0].startswith("ts"):
                continue
            try:
                cpu_s = row[3].strip().rstrip("%")
                cpu = float(cpu_s) if cpu_s else 0.0
            except Exception:
                cpu = 0.0
            max_cpu = max(max_cpu, cpu)
            # mem_usage like "3.77MiB / 187.4GiB"
            mu = row[4]
            try:
                used, total = [x.strip() for x in mu.split("/", 1)]
            except Exception:
                used, total = "", ""
            used_b = to_bytes(used)
            total_b = to_bytes(total)
            max_used = max(max_used, used_b)
            if total_b > 0:
                perc = used_b / total_b * 100.0
                max_mem_perc = max(max_mem_perc, perc)
    return (max_cpu, max_mem_perc, max_used)


def main() -> int:
    args = parse_args()
    in_csv = args.summary
    out_csv = args.out or in_csv
    rows = []
    with open(in_csv, newline="") as f:
        rd = csv.DictReader(f)
        fieldnames = rd.fieldnames or []
        for r in rd:
            rows.append(r)

    # Ensure columns exist
    for col in ("max_cpu_perc", "max_mem_perc", "max_mem_used_bytes"):
        if col not in fieldnames:
            fieldnames.append(col)

    for r in rows:
        art = r.get("artifacts_dir", "")
        if not art:
            continue
        stats_path = os.path.join(art, "docker_stats.csv")
        max_cpu, max_mem_perc, max_used = compute_from_stats(stats_path)
        if not math.isnan(max_cpu):
            r["max_cpu_perc"] = f"{max_cpu:.6f}"
        if not math.isnan(max_mem_perc):
            r["max_mem_perc"] = f"{max_mem_perc:.6f}"
        if not math.isnan(max_used):
            r["max_mem_used_bytes"] = f"{max_used:.0f}"

    with open(out_csv, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        wr.writerows(rows)
    print(f"[backfill] wrote {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
