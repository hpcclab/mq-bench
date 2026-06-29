#!/usr/bin/env python3
"""Merge multiple bursty fan-out summary_by_phase.csv files."""
import argparse
import csv
import os
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Run directories or summary_by_phase.csv files to merge",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output summary_by_phase.csv path",
    )
    return parser.parse_args()


def resolve_summary(path):
    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / "raw_data" / "summary_by_phase.csv"
    if not candidate.exists():
        raise FileNotFoundError(f"summary not found: {candidate}")
    return candidate


def main():
    args = parse_args()
    inputs = [resolve_summary(path) for path in args.inputs]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = None
    rows = []
    seen = set()
    for summary in inputs:
        with summary.open(newline="") as handle:
            reader = csv.DictReader(handle)
            if header is None:
                header = reader.fieldnames
            elif reader.fieldnames != header:
                raise ValueError(f"header mismatch in {summary}")

            for row in reader:
                key = (row.get("transport"), row.get("run_id"), row.get("phase"))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)

    if not header:
        raise ValueError("no input rows found")

    rows.sort(
        key=lambda row: (
            row.get("transport", ""),
            row.get("run_id", ""),
            float(row.get("phase_start_s") or 0),
        )
    )

    with out_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[merge] merged {len(rows)} rows from {len(inputs)} summaries")
    print(f"[merge] wrote {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()
