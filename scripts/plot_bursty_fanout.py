#!/usr/bin/env python3
"""Plot bursty fan-out phase summaries and raw time series."""
import argparse
import csv
import json
import math
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

from plot_results import (
    LATEX_AXIS_LABEL_SIZE,
    LATEX_DPI,
    LATEX_FIGSIZE,
    LATEX_FONT_SIZE,
    LATEX_LEGEND_SIZE,
    LATEX_TITLE_SIZE,
    style_for,
)
from summarize_bursty_fanout import fnum, parse_profile, read_rows, stats_value, timestamp

DEFAULT_PROFILE = "warmup:60:10,baseline:60:10,burst:60:30,elevated:60:15,recovery:60:10"
PLOT_LINEWIDTH = 1.8
PLOT_MARKER_SIZE = 4.5
PLOT_LEGEND_FONTSIZE = 5.5
PLOT_LEGEND_HANDLE_LENGTH = 0.9
PLOT_LEGEND_COLUMN_SPACING = 0.45
PLOT_LEGEND_HANDLE_TEXT_PAD = 0.25
PLOT_Y_LABEL_FONTSIZE = 8.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True, help="Path to summary_by_phase.csv")
    p.add_argument(
        "--out-dir",
        help="Output directory for plots (default: plots directory in the benchmark folder)",
    )
    p.add_argument("--profile", default=None)
    p.add_argument("--legend", action="store_true", help="Generate the standalone legend file; legends are kept out of individual plots")
    p.add_argument(
        "--latex",
        action="store_true",
        help="Write LaTeX-ready PDF plots and PNG siblings",
    )
    p.add_argument(
        "--inline-legend",
        action="store_true",
        help="Also draw legends inside each graph (disabled by default)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=None,
        help="Rolling-average window for all time-series plots; omit for CSV-derived auto tuning",
    )
    p.add_argument(
        "--throughput-window",
        type=int,
        default=None,
        help="Rolling-average window for delivery throughput plots; omit for CSV-derived auto tuning, use 1 to disable",
    )
    p.add_argument(
        "--latency-window",
        type=int,
        default=None,
        help="Rolling-average window for latency plots; omit for CSV-derived auto tuning, use 1 to disable",
    )
    p.add_argument(
        "--resource-window",
        type=int,
        default=None,
        help="Rolling-average window for CPU/memory/network plots; omit for CSV-derived auto tuning, use 1 to disable",
    )
    p.add_argument(
        "--bucket-seconds",
        type=float,
        default=1.0,
        help="Aggregate time-series plot points into fixed N-second buckets before plotting; use 1 to keep raw 1s points",
    )
    p.add_argument(
        "--marker-every",
        type=int,
        default=None,
        help="Draw a marker every N points on all time-series plots; omit for CSV-derived auto tuning, use 0 to hide markers",
    )
    p.add_argument(
        "--throughput-marker-every",
        type=int,
        default=None,
        help="Marker cadence for delivery throughput plots; omit for CSV-derived auto tuning",
    )
    p.add_argument(
        "--latency-marker-every",
        type=int,
        default=None,
        help="Marker cadence for latency plots; omit for CSV-derived auto tuning",
    )
    p.add_argument(
        "--resource-marker-every",
        type=int,
        default=None,
        help="Marker cadence for CPU/memory/network plots; omit for CSV-derived auto tuning",
    )
    p.add_argument(
        "--plot-points",
        help="Readable cached time-series CSV for plots (default: plot_points.csv next to the phase summary)",
    )
    p.add_argument(
        "--rebuild-plot-points",
        action="store_true",
        help="Rebuild the readable plot-points CSV from raw artifacts before plotting",
    )
    p.add_argument(
        "--latest-run-per-transport",
        action="store_true",
        help="When a transport appears multiple times, plot only its latest run and keep older summary rows ignored",
    )
    p.add_argument(
        "--throughput-y-scale",
        type=float,
        default=1_000_000.0,
        help="Scale factor for delivery throughput time-series y-axis",
    )
    p.add_argument(
        "--throughput-y-label",
        default="Throughput (million msgs/s)",
        help="Y-axis label for delivery throughput time-series plot",
    )
    p.add_argument(
        "--start-phase",
        default=None,
        help="Start time-series plots at a phase label such as B5, burst 5, or recovery 4; earlier points are hidden",
    )
    return p.parse_args()


def configure_plot_style(latex: bool):
    if not latex:
        return ".png", 150
    plt.rcParams.update({
        "font.size": LATEX_FONT_SIZE,
        "axes.titlesize": LATEX_TITLE_SIZE,
        "axes.labelsize": LATEX_AXIS_LABEL_SIZE,
        "xtick.labelsize": LATEX_FONT_SIZE - 2,
        "ytick.labelsize": LATEX_FONT_SIZE - 2,
        "legend.fontsize": LATEX_LEGEND_SIZE,
        "figure.figsize": LATEX_FIGSIZE,
        "figure.dpi": LATEX_DPI,
        "savefig.dpi": LATEX_DPI,
        "lines.linewidth": PLOT_LINEWIDTH,
        "lines.markersize": PLOT_MARKER_SIZE,
        "axes.linewidth": 0.8,
        "grid.linewidth": 0.5,
        "font.family": "serif",
        "mathtext.fontset": "cm",
    })
    return ".pdf", LATEX_DPI


def save_fig(fig, out_dir, filename, ext, dpi):
    stem, _ = os.path.splitext(filename)
    primary = stem + ext
    path = os.path.join(out_dir, primary)
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    if ext == ".pdf":
        png_name = stem + ".png"
        fig.savefig(os.path.join(out_dir, png_name), dpi=dpi, bbox_inches="tight")
        return png_name
    return primary


def transport_style(label):
    marker, _linestyle, _linewidth, color = style_for(label)
    return marker, color


def display_transport_label(label):
    names = {
        "mqtt_mosquitto": "Mosquitto",
        "mosquitto": "Mosquitto",
        "mqtt_hivemq": "HiveMQ",
        "hivemq": "HiveMQ",
        "mqtt_emqx": "EMQX",
        "emqx": "EMQX",
        "mqtt_artemis": "Artemis",
        "artemis": "Artemis",
    }
    return names.get(label, label)


def phase_name_looks_valid(value):
    lower = (value or "").strip().lower()
    return lower in {"warmup", "baseline", "recovery", "elevated"} or is_burst_phase(lower)


def infer_transport_from_values(values):
    haystack = " ".join(values).lower()
    transports = [
        "mqtt_mosquitto",
        "mqtt_rabbitmq",
        "mqtt_artemis",
        "mqtt_hivemq",
        "mqtt_emqx",
        "mosquitto",
        "rabbitmq",
        "artemis",
        "hivemq",
        "emqx",
        "zenoh",
        "redis",
        "nats",
    ]
    for transport in transports:
        token = transport.replace("mqtt_", "")
        if f"_{transport}_" in haystack or f"/{transport}_" in haystack or f"_{token}_" in haystack:
            return transport
    return ""


def read_summary(path: str):
    rows = []
    repaired = 0
    skipped = 0
    with open(path, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return rows
        for values in reader:
            if not values:
                continue
            if len(values) == len(header) - 1 and len(values) > 6 and phase_name_looks_valid(values[5]):
                transport = infer_transport_from_values(values)
                if transport:
                    values = [transport] + values
                    repaired += 1
            if len(values) != len(header):
                skipped += 1
                continue
            row = dict(zip(header, values))
            if not phase_name_looks_valid(row.get("phase", "")):
                skipped += 1
                continue
            rows.append(row)
    if repaired:
        print(f"[plot] Repaired {repaired} shifted summary row(s)")
    if skipped:
        print(f"[plot] Skipped {skipped} malformed summary row(s)")
    return rows


def transport_artifacts(rows, summary_path=None):
    seen = {}
    summary_raw_dir = os.path.dirname(os.path.abspath(summary_path)) if summary_path else ""
    for row in rows:
        art = row.get("artifacts_dir", "")
        transport = row.get("transport", "")
        if not art or not transport:
            continue
        resolved = resolve_artifacts_dir(art, row.get("run_id", ""), summary_raw_dir)
        if resolved and resolved not in seen:
            seen[resolved] = transport
    return seen


def resolve_artifacts_dir(art_dir, run_id, summary_raw_dir=""):
    if art_dir and os.path.isdir(art_dir):
        return art_dir

    candidates = []
    if summary_raw_dir:
        if run_id:
            candidates.append(os.path.join(summary_raw_dir, run_id, "fanout_singlesite"))
        if art_dir:
            parts = os.path.normpath(art_dir).split(os.sep)
            if "raw_data" in parts:
                idx = len(parts) - 1 - parts[::-1].index("raw_data")
                suffix = parts[idx + 1:]
                if suffix:
                    candidates.append(os.path.join(summary_raw_dir, *suffix))
            if run_id:
                candidates.append(os.path.join(os.path.dirname(summary_raw_dir), "artifacts", run_id, "fanout_singlesite"))

    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return candidate
    return art_dir


def read_early_stop_file(art_dir):
    path = os.path.join(art_dir, "EARLY_STOP.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def early_stop_markers(rows, summary_path, warmup_offset):
    markers = {}
    for art_dir, transport in transport_artifacts(rows, summary_path).items():
        data = read_early_stop_file(art_dir)
        if not data:
            continue
        stop_elapsed = optional_float(data, "stop_elapsed_s")
        if stop_elapsed is None:
            continue
        failed_phase = data.get("failed_phase") or data.get("failed_phase_name") or "failure"
        markers[transport] = {
            "x": max(0.0, stop_elapsed - warmup_offset),
            "label": f"failed at {failed_phase}",
            "reason": data.get("reason", "failed"),
        }
    return markers


def filter_latest_run_per_transport(rows):
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        transport = row.get("transport", "")
        if not transport:
            continue
        run_id = row.get("run_id", "") or row.get("artifacts_dir", "") or "__unknown__"
        grouped[transport][run_id].append(row)

    keep_keys = set()
    for transport, runs in grouped.items():
        if len(runs) <= 1:
            keep_keys.update((transport, run_id) for run_id in runs)
            continue
        latest_run_id = max(
            runs,
            key=lambda run_id: max((optional_float(row, "phase_end_s") or 0.0) for row in runs[run_id]),
        )
        keep_keys.add((transport, latest_run_id))

    if not keep_keys:
        return rows

    filtered = []
    for row in rows:
        transport = row.get("transport", "")
        run_id = row.get("run_id", "") or row.get("artifacts_dir", "") or "__unknown__"
        if (transport, run_id) in keep_keys:
            filtered.append(row)
    return filtered


def complete_phases_from_profile(summary_rows, profile):
    summary_phases = phases_from_summary(summary_rows)
    if profile:
        profile_phases = parse_profile(profile)
        if profile_phases:
            return profile_phases
    if summary_phases:
        return summary_phases
    return parse_profile(DEFAULT_PROFILE)


def first_active_pub_ts(art_dir: str) -> float:
    candidates = []
    for name in os.listdir(art_dir) if os.path.isdir(art_dir) else []:
        if name.startswith("pub_") and name.endswith(".csv"):
            for row in read_rows(os.path.join(art_dir, name)):
                if fnum(row, "sent_count", 0.0) > 0:
                    candidates.append(timestamp(row))
                    break
    if candidates:
        return min(candidates)
    rows = read_rows(os.path.join(art_dir, "sub_agg.csv"))
    ts = [timestamp(r) for r in rows if timestamp(r) > 0]
    return min(ts) if ts else 0.0




def summary_group_key(row):
    return row.get("run_id", "") or row.get("transport", "")


def grouped_summary_rows(rows):
    grouped = defaultdict(list)
    for row in rows:
        key = summary_group_key(row)
        if key:
            grouped[key].append(row)
    return grouped


def normalized_phases_for_rows(rows):
    valid_rows = []
    offset = None
    for row in rows:
        if not phase_name_looks_valid(row.get("phase", "")):
            continue
        start = optional_float(row, "phase_start_s")
        end = optional_float(row, "phase_end_s")
        rate = optional_float(row, "rate_per_pub")
        if start is None or end is None or rate is None or end <= start:
            continue
        valid_rows.append((start, end, row.get("phase", ""), rate))
        offset = start if offset is None else min(offset, start)
    if offset is None:
        return []

    phases = []
    seen = set()
    for raw_start, raw_end, name, rate in sorted(valid_rows, key=lambda item: (item[0], item[1], item[2])):
        start = raw_start - offset
        end = raw_end - offset
        key = (name.strip().lower(), round(start, 6), round(end, 6), round(rate, 6))
        if key in seen:
            continue
        phases.append({"name": name, "start": start, "end": end, "rate": rate})
        seen.add(key)
    return phases


def phases_from_summary(rows):
    candidates = [normalized_phases_for_rows(group) for group in grouped_summary_rows(rows).values()]
    candidates = [phases for phases in candidates if phases]
    if not candidates:
        return []
    return max(candidates, key=lambda phases: (len(phases), phases[-1]["end"] - phases[0]["start"]))


def warmup_end(phases):
    for phase in phases:
        if phase["name"].lower() == "warmup":
            return phase["end"]
    return 0.0


def phases_after_warmup(phases):
    offset = warmup_end(phases)
    visible = []
    for phase in phases:
        if phase["end"] <= offset:
            continue
        visible.append({
            "name": phase["name"],
            "start": max(0.0, phase["start"] - offset),
            "end": phase["end"] - offset,
            "rate": phase["rate"],
        })
    return visible


def shift_series_after(series, offset):
    shifted = defaultdict(list)
    for label, points in series.items():
        for x, y in points:
            if x >= offset:
                shifted[label].append((x - offset, y))
    return shifted


def trim_series_until(series, end):
    if end <= 0:
        return series
    trimmed = defaultdict(list)
    for label, points in series.items():
        trimmed[label] = [(x, y) for x, y in points if x <= end]
    return trimmed


def bucket_duplicate_timestamps(series):
    bucketed = defaultdict(list)
    for label, points in series.items():
        by_x = defaultdict(list)
        for x, y in points:
            by_x[x].append(y)
        for x in sorted(by_x):
            ys = by_x[x]
            bucketed[label].append((x, sum(ys) / len(ys)))
    return bucketed


def bucket_time_series(series, bucket_seconds):
    if bucket_seconds <= 1:
        return series
    bucketed = defaultdict(list)
    for label, points in series.items():
        buckets = defaultdict(list)
        for x, y in points:
            bucket_start = math.floor(float(x) / bucket_seconds) * bucket_seconds
            buckets[bucket_start].append(float(y))
        for bucket_start in sorted(buckets):
            values = buckets[bucket_start]
            bucket_mid = bucket_start + (bucket_seconds / 2.0)
            bucketed[label].append((bucket_mid, sum(values) / len(values)))
    return bucketed


def bucket_series_map(series_map, bucket_seconds):
    if bucket_seconds <= 1:
        return series_map
    return {key: bucket_time_series(series, bucket_seconds) for key, series in series_map.items()}


def rolling_average_series(series, window):
    if window <= 1:
        return series
    smoothed = defaultdict(list)
    half_left = (window - 1) // 2
    half_right = window // 2
    for label, points in series.items():
        points = sorted(points, key=lambda point: point[0])
        values = [y for _x, y in points]
        for idx, (x, _y) in enumerate(points):
            start = max(0, idx - half_left)
            end = min(len(values), idx + half_right + 1)
            window_values = values[start:end]
            smoothed[label].append((x, sum(window_values) / len(window_values)))
    return smoothed



def terminal_sparse_tail_cutoff(points):
    points = sorted(points, key=lambda point: point[0])
    if len(points) < 4:
        return points[-1][0] if points else 0.0
    gaps = [b[0] - a[0] for a, b in zip(points, points[1:]) if b[0] > a[0]]
    if not gaps:
        return points[-1][0]
    sorted_gaps = sorted(gaps)
    median_gap = sorted_gaps[len(sorted_gaps) // 2]
    sparse_threshold = max(median_gap * 1.5, median_gap + 1.0)
    sparse_suffix_start = len(gaps)
    while sparse_suffix_start > 0 and gaps[sparse_suffix_start - 1] >= sparse_threshold:
        sparse_suffix_start -= 1
    sparse_suffix_len = len(gaps) - sparse_suffix_start
    if sparse_suffix_len >= 2:
        return points[sparse_suffix_start][0]
    return points[-1][0]


def median_series_step(points):
    gaps = [b[0] - a[0] for a, b in zip(points, points[1:]) if b[0] > a[0]]
    if not gaps:
        return 1.0
    gaps = sorted(gaps)
    return max(gaps[len(gaps) // 2], 1.0)


def append_zero_tail(points, start, end, step):
    tail = []
    x = start
    if not points or points[-1] != (start, 0.0):
        tail.append((start, 0.0))
    x += step
    while x < end:
        tail.append((round(x, 6), 0.0))
        x += step
    if not tail or tail[-1][0] != end:
        tail.append((end, 0.0))
    return tail


def extend_missing_series_with_zero(series, end, failed_labels=None):
    failed_labels = set(failed_labels or [])
    if end <= 0:
        return series
    extended = defaultdict(list)
    for label, points in series.items():
        points = sorted(points, key=lambda point: point[0])
        if label in failed_labels:
            extended[label] = list(points)
            continue
        if not points:
            extended[label].append((0.0, 0.0))
            extended[label].extend(append_zero_tail([], 0.0, end, 1.0))
            continue
        cutoff_x = terminal_sparse_tail_cutoff(points)
        kept = [(x, y) for x, y in points if x <= cutoff_x]
        extended[label] = kept
        if cutoff_x < end:
            step = median_series_step(kept)
            if kept and kept[-1][1] != 0.0:
                extended[label].append((cutoff_x, 0.0))
            extended[label].extend(append_zero_tail(extended[label], cutoff_x, end, step))
    return extended


def extend_missing_series_with_last(series, end, failed_labels=None):
    failed_labels = set(failed_labels or [])
    if end <= 0:
        return series
    extended = defaultdict(list)
    for label, points in series.items():
        points = sorted(points, key=lambda point: point[0])
        if not points:
            continue
        extended[label] = list(points)
        if label in failed_labels:
            continue
        last_x, last_y = points[-1]
        if last_x < end:
            extended[label].append((end, last_y))
    return extended


def series_has_nonpositive(series, y_scale=1.0):
    for points in series.values():
        for _x, y in points:
            if y / y_scale <= 0:
                return True
    return False



def _series_counts(series):
    return [len(points) for points in series.values() if points]


def _median(values, default=0.0):
    values = sorted(values)
    if not values:
        return default
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def series_duration(series):
    xs = [x for points in series.values() for x, _y in points]
    if len(xs) < 2:
        return 0.0
    return max(xs) - min(xs)


def median_sample_interval(series):
    gaps = []
    for points in series.values():
        xs = sorted({x for x, _y in points})
        gaps.extend(b - a for a, b in zip(xs, xs[1:]) if b > a)
    return max(_median(gaps, 1.0), 1.0)


def shortest_phase_duration(phases, fallback=0.0):
    durations = [phase["end"] - phase["start"] for phase in phases if phase["end"] > phase["start"]]
    if durations:
        return min(durations)
    return fallback if fallback > 0 else 60.0


def representative_series(*series_list):
    selected = defaultdict(list)
    for series in series_list:
        for label, points in series.items():
            if len(points) > len(selected[label]):
                selected[label] = list(points)
    return selected


def _odd_window(points):
    points = max(1, int(round(points)))
    if points > 1 and points % 2 == 0:
        points += 1
    return points


def auto_smoothing_window(metric, series, phases):
    counts = _series_counts(series)
    if not counts:
        return 1
    max_points = max(counts)
    if max_points < 12:
        return 1

    step = median_sample_interval(series)
    short_phase = shortest_phase_duration(phases, series_duration(series))
    if metric == "throughput":
        span_s = min(max(3.0 * step, 0.08 * short_phase), max(5.0 * step, 8.0))
        metric_cap = 21
    elif metric == "latency":
        span_s = min(max(step, 0.04 * short_phase), max(3.0 * step, 5.0))
        metric_cap = 15
    elif metric == "resource":
        span_s = min(max(5.0 * step, 0.12 * short_phase), max(7.0 * step, 15.0))
        metric_cap = 31
    else:
        span_s = max(step, 0.05 * short_phase)
        metric_cap = 21

    window = _odd_window(span_s / step)
    point_cap = max(1, max_points // 6)
    return max(1, min(window, metric_cap, point_cap))


def choose_smoothing_window(metric, series, phases, explicit_value=None, common_value=None):
    if explicit_value is not None:
        return max(1, explicit_value)
    if common_value is not None:
        return max(1, common_value)
    return auto_smoothing_window(metric, series, phases)


def auto_marker_every(metric, series):
    counts = _series_counts(series)
    if not counts:
        return 0
    max_points = max(counts)
    label_count = len(counts)
    targets = {"throughput": 55, "latency": 48, "resource": 40}
    target = targets.get(metric, 50)
    if label_count >= 8:
        target = max(20, int(target * 0.70))
    elif label_count >= 5:
        target = max(24, int(target * 0.82))
    if max_points <= target:
        return 1
    return max(1, min(60, (max_points + target - 1) // target))


def choose_marker_every(metric, series, explicit_value=None, common_value=None):
    if explicit_value is not None:
        return max(0, explicit_value)
    if common_value is not None:
        return max(0, common_value)
    return auto_marker_every(metric, series)


def standalone_legend(out_dir, labels, ext, dpi, include_target=False):
    labels = sorted({label for label in labels if label})
    handles = []
    legend_labels = []
    for label in labels:
        marker, color = transport_style(label)
        handles.append(Line2D([0], [0], marker=marker, linestyle='-', linewidth=PLOT_LINEWIDTH, color=color, markersize=PLOT_MARKER_SIZE + 2, markerfacecolor=color))
        legend_labels.append(display_transport_label(label))
    if include_target:
        handles.append(Line2D([0], [0], linestyle='--', linewidth=1.5, color='black', alpha=0.65))
        legend_labels.append('Fan-out target')
    if not handles:
        return None
    ncol = min(4, len(handles))
    rows = (len(handles) + ncol - 1) // ncol
    fig_width = max(7.0, 2.0 * ncol)
    fig_height = max(0.55, 0.42 * rows)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis('off')
    fig.legend(
        handles,
        legend_labels,
        loc='center',
        ncol=ncol,
        frameon=False,
        fontsize=PLOT_LEGEND_FONTSIZE if ext != '.pdf' else LATEX_LEGEND_SIZE,
        handlelength=1.8,
        columnspacing=1.0,
        handletextpad=0.35,
    )
    fig.tight_layout(pad=0.05)
    pdf_path = os.path.join(out_dir, 'legend.pdf')
    png_path = os.path.join(out_dir, 'legend.png')
    fig.savefig(pdf_path, dpi=dpi, bbox_inches='tight', pad_inches=0.05)
    fig.savefig(png_path, dpi=dpi, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    return 'legend.png'

def target_delivery_multiplier(rows):
    for row in rows:
        rate = optional_float(row, "rate_per_pub")
        delivery_rate = optional_float(row, "delivery_rate")
        if rate is not None and rate > 0 and delivery_rate is not None:
            return delivery_rate / rate
    return None


def target_delivery_series(rows, warmup_offset, phases=None):
    multiplier = target_delivery_multiplier(rows)
    if phases and multiplier is not None:
        points = []
        for phase in phases:
            name = phase.get("name", "")
            if not name or name.lower() == "warmup":
                continue
            start = phase["start"]
            end = phase["end"]
            if end <= 0:
                continue
            target = phase.get("rate", 0.0) * multiplier
            points.extend([(max(0.0, start), target), (end, target)])
        return {"Fan-out target": points} if points else {}

    points = []
    seen = set()
    for row in rows:
        phase = row.get("phase", "")
        if not phase or phase.lower() == "warmup":
            continue
        try:
            raw_start = float(row.get("phase_start_s", ""))
            raw_end = float(row.get("phase_end_s", ""))
            start = raw_start - warmup_offset
            end = raw_end - warmup_offset
            target = float(row.get("delivery_rate", ""))
        except ValueError:
            continue
        key = (phase, raw_start, raw_end)
        if key in seen:
            continue
        if end <= 0:
            continue
        points.extend([(max(0.0, start), target), (end, target)])
        seen.add(key)
    return {"Fan-out target": points} if points else {}

def is_burst_phase(name):
    lower = (name or "").strip().lower()
    if lower == "burst" or lower.startswith("burst"):
        return True
    return len(lower) > 1 and lower[0] == "b" and lower[1].isdigit()


def phase_label(phase):
    label = phase["name"]
    rate = phase.get("rate")
    if rate is None or not is_burst_phase(label):
        return label
    return f"{label}\n{fmt_plot_number(rate)}/s"


def fmt_compact_rate(value):
    if value is None:
        return ""
    value = float(value)
    if abs(value) >= 1000:
        return f"{fmt_plot_number(value / 1000.0)}k"
    return fmt_plot_number(value)


def phases_with_compact_labels(phases):
    labeled = []
    baseline_labeled = False
    burst_index = 0
    recovery_index = 0
    for phase in phases:
        item = dict(phase)
        name = (phase.get("name") or "").strip().lower()
        item["rate_label"] = fmt_compact_rate(phase.get("rate"))
        if is_burst_phase(name):
            burst_index += 1
            item["compact_label"] = f"B{burst_index}"
            item["burst_level"] = item["rate_label"]
        elif not baseline_labeled and name == "baseline":
            item["compact_label"] = "Baseline"
            baseline_labeled = True
        elif name == "recovery":
            recovery_index += 1
            item["compact_label"] = f"R{recovery_index}"
        labeled.append(item)
    if not baseline_labeled:
        for item in labeled:
            name = (item.get("name") or "").strip().lower()
            if name not in ("warmup", "recovery") and not is_burst_phase(name):
                item["compact_label"] = "Baseline"
                break
    return labeled


def compact_phase_label_items(phases):
    items = []
    rate_items = []
    baseline_labeled = False
    burst_index = 0
    recovery_index = 0
    for phase in phases:
        name = (phase.get("name") or "").strip().lower()
        mid = (phase["start"] + phase["end"]) / 2.0
        rate_label = phase.get("rate_label", fmt_compact_rate(phase.get("rate")))
        compact_label = phase.get("compact_label")
        if compact_label:
            items.append((mid, compact_label))
            rate_items.append((mid, rate_label))
            if compact_label == "Baseline":
                baseline_labeled = True
            continue
        if is_burst_phase(name):
            burst_index += 1
            label = f"B{burst_index}"
            items.append((mid, label))
            rate_items.append((mid, rate_label))
        elif not baseline_labeled and name == "baseline":
            items.append((mid, "Baseline"))
            rate_items.append((mid, rate_label))
            baseline_labeled = True
        elif name == "recovery":
            recovery_index += 1
            items.append((mid, f"R{recovery_index}"))
            rate_items.append((mid, rate_label))
    if not baseline_labeled:
        for phase in phases:
            name = (phase.get("name") or "").strip().lower()
            if name not in ("warmup", "recovery") and not is_burst_phase(name):
                mid = (phase["start"] + phase["end"]) / 2.0
                items.insert(0, (mid, "Baseline"))
                rate_items.insert(0, (mid, phase.get("rate_label", fmt_compact_rate(phase.get("rate")))))
                break
    return items, rate_items


def normalize_phase_selector(value):
    return (value or "").strip().lower().replace("_", " ").replace("-", " ")


def phase_selector_aliases(phase, burst_index, repeated_counts):
    name = (phase.get("name") or "").strip()
    lower_name = name.lower()
    aliases = {normalize_phase_selector(name)}
    count = repeated_counts[lower_name]
    if count > 1:
        aliases.add(normalize_phase_selector(f"{name} {count}"))
    if lower_name == "baseline":
        aliases.add("baseline")
    if lower_name == "recovery":
        aliases.add(normalize_phase_selector(f"recovery {count}"))
    if is_burst_phase(lower_name):
        aliases.add(normalize_phase_selector(f"B{burst_index}"))
        aliases.add(normalize_phase_selector(f"burst {burst_index}"))
    return aliases


def start_offset_for_phase(phases, selector):
    wanted = normalize_phase_selector(selector)
    if not wanted:
        return 0.0
    repeated_counts = defaultdict(int)
    burst_index = 0
    for idx, phase in enumerate(phases):
        lower_name = (phase.get("name") or "").strip().lower()
        repeated_counts[lower_name] += 1
        if is_burst_phase(lower_name):
            burst_index += 1
        if wanted in phase_selector_aliases(phase, burst_index, repeated_counts):
            if is_burst_phase(lower_name):
                for previous in reversed(phases[:idx]):
                    previous_name = (previous.get("name") or "").strip().lower()
                    if previous_name in {"baseline", "recovery"}:
                        return previous["start"]
            return phase["start"]
    available = []
    repeated_counts = defaultdict(int)
    burst_index = 0
    for phase in phases:
        lower_name = (phase.get("name") or "").strip().lower()
        repeated_counts[lower_name] += 1
        if is_burst_phase(lower_name):
            burst_index += 1
            available.append(f"B{burst_index}")
        elif lower_name == "baseline":
            available.append("baseline")
        elif lower_name == "recovery":
            available.append(f"recovery {repeated_counts[lower_name]}")
    raise SystemExit(
        f"[plot] Unknown --start-phase {selector!r}; available examples: {', '.join(available)}"
    )


def rebase_phases_from(phases, start_offset):
    if start_offset <= 0:
        return phases
    rebased = []
    for phase in phases:
        if phase["end"] <= start_offset:
            continue
        shifted = dict(phase)
        shifted["start"] = max(0.0, phase["start"] - start_offset)
        shifted["end"] = phase["end"] - start_offset
        rebased.append(shifted)
    return rebased


def rebase_series_from(series, start_offset):
    if start_offset <= 0:
        return series
    rebased = defaultdict(list)
    for label, points in series.items():
        rebased[label] = [(x - start_offset, y) for x, y in points if x >= start_offset]
    return rebased


def rebase_series_map_from(series_map, start_offset):
    if start_offset <= 0:
        return series_map
    return {key: rebase_series_from(series, start_offset) for key, series in series_map.items()}


def rebase_failure_markers_from(failures, start_offset):
    if start_offset <= 0 or not failures:
        return failures
    rebased = {}
    for label, failure in failures.items():
        shifted = dict(failure)
        shifted["x"] = max(0.0, float(shifted.get("x", 0.0)) - start_offset)
        rebased[label] = shifted
    return rebased



def add_phase_lines(ax, phases, latex=False, show_boundaries=True, compact_labels=False):
    if not phases:
        return
    for phase in phases:
        if show_boundaries:
            ax.axvline(phase["start"], color="#999999", linewidth=0.8, linestyle="--", alpha=0.6)

    if compact_labels:
        label_items, rate_items = compact_phase_label_items(phases)
    else:
        label_items = [((phase["start"] + phase["end"]) / 2.0, phase_label(phase)) for phase in phases]
        rate_items = []

    label_fontsize = 7.6 if latex else 8.5
    if compact_labels:
        ax.text(
            0.5,
            1.106,
            "Phase · Offered rate per publisher (msg/s)",
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            fontsize=10.0 if latex else 8.0,
            color=ax.xaxis.label.get_color(),
            clip_on=False,
        )
    for mid, label in label_items:
        ax.text(
            mid,
            1.055 if compact_labels else 1.025,
            label,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=label_fontsize,
            color=ax.xaxis.label.get_color(),
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.72, "pad": 1.0},
            clip_on=False,
        )

    if compact_labels:
        for mid, rate in rate_items:
            ax.text(
                mid,
                1.025,
                rate,
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="bottom",
                fontsize=label_fontsize,
                color=ax.xaxis.label.get_color(),
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.72, "pad": 1.0},
                clip_on=False,
            )

    if show_boundaries:
        ax.axvline(phases[-1]["end"], color="#999999", linewidth=0.8, linestyle="--", alpha=0.6)


def marker_sampled_points(points, marker_every):
    if marker_every is None or marker_every <= 1:
        return points
    if not points:
        return points
    sampled = points[::marker_every]
    if sampled[-1] != points[-1]:
        sampled.append(points[-1])
    return sampled


def marker_sample_indices(points, marker_every):
    if marker_every is None or marker_every <= 1:
        return None
    if marker_every == 0 or not points:
        return []
    indices = list(range(0, len(points), marker_every))
    last_index = len(points) - 1
    if indices[-1] != last_index:
        indices.append(last_index)
    return indices


def anchor_axes_at_zero(ax, x_max=None, y_min=0.0, hide_y_zero_label=False, y_top_padding=0.12):
    """Place the zero origin at the lower-left corner of generated plots."""
    ax.margins(x=0, y=0)
    if x_max is not None and x_max > 0:
        ax.set_xlim(left=0.0, right=x_max)
    else:
        ax.set_xlim(left=0.0)
    ax.set_ylim(bottom=y_min)
    bottom, top = ax.get_ylim()
    if y_top_padding > 0 and top > bottom:
        ax.set_ylim(top=top + ((top - bottom) * y_top_padding))
    if hide_y_zero_label:
        def one_origin_zero(value, _position):
            if abs(value) < 1e-12:
                return ""
            return f"{value:g}"

        ax.yaxis.set_major_formatter(FuncFormatter(one_origin_zero))


def nearest_y_at_or_before(points, x):
    selected = None
    for px, py in sorted(points, key=lambda point: point[0]):
        if px <= x:
            selected = (px, py)
        else:
            break
    if selected is not None:
        return selected[1]
    return points[0][1] if points else 0.0


def save_line_plot(out_dir, filename, title, ylabel, series, phases, ext, dpi, legend=False, log_y=False, y_scale=1.0, target_series=None, marker_every=None, step=False, y_min=None, compact_phase_labels=False, failure_markers=None, caption=None):
    if not series:
        return None
    marker_every = 0 if marker_every is None else marker_every
    latex = ext == ".pdf"
    has_target = bool(target_series)
    x_values = []
    fig, ax = plt.subplots(figsize=LATEX_FIGSIZE if ext == ".pdf" else (10, 5.5))
    for label, points in sorted(series.items()):
        points = sorted(points, key=lambda point: point[0])
        if not points:
            continue
        x_values.extend(p[0] for p in points)
        marker, color = transport_style(label)
        ax.plot(
            [p[0] for p in points],
            [p[1] / y_scale for p in points],
            marker=marker if marker_every != 0 else None,
            markevery=marker_sample_indices(points, marker_every),
            linestyle="-",
            markersize=PLOT_MARKER_SIZE,
            linewidth=2.2 if has_target else PLOT_LINEWIDTH,
            color=color,
            label=display_transport_label(label),
            drawstyle="steps-post" if step else "default",
        )
    if target_series:
        for label, points in sorted(target_series.items()):
            if not points:
                continue
            xs = [p[0] for p in points]
            ys = [p[1] / y_scale for p in points]
            x_values.extend(xs)
            ax.step(
                xs,
                ys,
                where="post",
                linestyle="--",
                linewidth=1.5,
                color="black",
                alpha=0.65,
                label=label,
            )
    if failure_markers:
        for label, failure in failure_markers.items():
            points = sorted(series.get(label, []), key=lambda point: point[0])
            if not points:
                continue
            marker, color = transport_style(label)
            fail_x = failure.get("x", points[-1][0])
            fail_x = min(max(fail_x, points[0][0]), points[-1][0])
            fail_y = nearest_y_at_or_before(points, fail_x) / y_scale
            ax.scatter([fail_x], [fail_y], marker="x", s=42, linewidths=1.6, color=color, zorder=6)
            ax.annotate(
                failure.get("label", "failed"),
                xy=(fail_x, fail_y),
                xytext=(4, 6),
                textcoords="offset points",
                fontsize=7.0 if latex else 8.0,
                color=color,
                ha="left",
                va="bottom",
                clip_on=True,
            )
    ax.set_xlabel("Time (s)", fontsize = PLOT_X_LABEL_FONTSIZE if not latex else LATEX_AXIS_LABEL_SIZE - 7)
    ax.set_ylabel(ylabel, fontsize=PLOT_Y_LABEL_FONTSIZE if not latex else LATEX_AXIS_LABEL_SIZE - 7)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
        ax.set_yscale("symlog", linthresh=1.0)
    add_phase_lines(ax, phases, latex, show_boundaries=not has_target, compact_labels=compact_phase_labels)
    if phases:
        x_max = max(phase["end"] for phase in phases)
    elif x_values:
        x_max = max(x_values)
    else:
        x_max = None
    anchor_axes_at_zero(ax, x_max=x_max, y_min=0.0 if y_min is None else y_min, hide_y_zero_label=True)
    if legend:
        ax.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=min(4, max(1, len(series) + len(target_series or {}))),
            frameon=False,
            fontsize=PLOT_LEGEND_FONTSIZE,
            handlelength=PLOT_LEGEND_HANDLE_LENGTH,
            columnspacing=PLOT_LEGEND_COLUMN_SPACING,
            handletextpad=PLOT_LEGEND_HANDLE_TEXT_PAD,
        )
    if caption:
        fig.text(
            0.125,
            0.015,
            caption,
            ha="left",
            va="bottom",
            fontsize=7.0 if latex else 8.0,
            color="#333333",
        )
    if compact_phase_labels:
        fig.subplots_adjust(top=0.84, bottom=0.16 if caption else 0.11)
    else:
        fig.tight_layout(rect=(0, 0.08, 1, 1) if caption else None)
    gallery_file = save_fig(fig, out_dir, filename, ext, dpi)
    plt.close(fig)
    return gallery_file


def time_series_from_sub(art_dir, run_start):
    rows = read_rows(os.path.join(art_dir, "sub_agg.csv"))
    throughput = []
    p99 = []
    p95 = []
    p50 = []
    mean = []
    prev_count = 0.0
    prev_mean = 0.0
    for row in rows:
        rel = timestamp(row) - run_start
        cumulative_count = fnum(row, "latency_sample_count", 0.0)
        cumulative_mean = fnum(row, "latency_ns_mean", 0.0)
        if rel < 0:
            prev_count = cumulative_count
            prev_mean = cumulative_mean
            continue
        throughput.append((rel, fnum(row, "interval_throughput", 0.0)))
        has_interval_latency = "interval_latency_sample_count" in row
        sample_count = fnum(row, "interval_latency_sample_count", 0.0)

        if has_interval_latency:
            if sample_count > 0:
                p50.append((rel, fnum(row, "interval_latency_ns_p50", 0.0) / 1_000_000.0))
                p95.append((rel, fnum(row, "interval_latency_ns_p95", 0.0) / 1_000_000.0))
                p99.append((rel, fnum(row, "interval_latency_ns_p99", 0.0) / 1_000_000.0))
                mean.append((rel, fnum(row, "interval_latency_ns_mean", 0.0) / 1_000_000.0))
            else:
                p50.append((rel, 0.0))
                p95.append((rel, 0.0))
                p99.append((rel, 0.0))
                mean.append((rel, 0.0))
        else:
            p50.append((rel, fnum(row, "latency_ns_p50", 0.0) / 1_000_000.0))
            p95.append((rel, fnum(row, "latency_ns_p95", 0.0) / 1_000_000.0))
            p99.append((rel, fnum(row, "latency_ns_p99", 0.0) / 1_000_000.0))
            delta_count = cumulative_count - prev_count
            delta_total = (cumulative_mean * cumulative_count) - (prev_mean * prev_count)
            mean_ns = delta_total / delta_count if delta_count > 0 and delta_total > 0 else 0.0
            mean.append((rel, mean_ns / 1_000_000.0))
        prev_count = cumulative_count
        prev_mean = cumulative_mean
    return throughput, p50, p95, p99, mean


def time_series_from_stats(art_dir, run_start):
    rows = read_rows(os.path.join(art_dir, "docker_stats.csv"))
    by_ts = defaultdict(lambda: {"cpu": [], "mem": [], "rx": 0.0, "tx": 0.0})
    for row in rows:
        rel = timestamp(row) - run_start
        if rel < 0:
            continue
        bucket = by_ts[rel]
        bucket["cpu"].append(stats_value(row, "cpu_perc_num", "cpu_perc") / 100.0)
        bucket["mem"].append(stats_value(row, "mem_used_b", "mem_usage") / (1024.0 ** 3))
        bucket["rx"] += stats_value(row, "net_rx_b")
        bucket["tx"] += stats_value(row, "net_tx_b")

    cpu = []
    mem = []
    rx_bps = []
    tx_bps = []
    prev = None
    for rel in sorted(by_ts):
        data = by_ts[rel]
        if data["cpu"]:
            cpu.append((rel, sum(data["cpu"]) / len(data["cpu"])))
        if data["mem"]:
            mem.append((rel, sum(data["mem"]) / len(data["mem"])))
        if prev is not None and rel > prev[0]:
            dt = rel - prev[0]
            drx = data["rx"] - prev[1]
            dtx = data["tx"] - prev[2]
            if drx >= 0 and dtx >= 0:
                rx_bps.append((rel, (drx * 8.0) / dt))
                tx_bps.append((rel, (dtx * 8.0) / dt))
        prev = (rel, data["rx"], data["tx"])
    return cpu, mem, rx_bps, tx_bps



PLOT_POINTS_FILENAME = "plot_points.csv"
PLOT_POINT_FIELDS = [
    "transport",
    "run_id",
    "time_s",
    "phase",
    "phase_time_s",
    "target_throughput_msg_s",
    "delivery_throughput_msg_s",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "avg_latency_ms",
    "cpu_cores",
    "memory_gb",
    "network_rx_gbps",
    "network_tx_gbps",
]


def default_plot_points_path(summary_path, bucket_seconds=1.0):
    if bucket_seconds > 1:
        suffix = fmt_plot_number(bucket_seconds).replace(".", "p")
        filename = f"plot_points_{suffix}s.csv"
    else:
        filename = PLOT_POINTS_FILENAME
    return os.path.join(os.path.dirname(os.path.abspath(summary_path)), filename)


def optional_float(row, key):
    raw = row.get(key, "")
    if raw is None or raw == "":
        return None
    try:
        return float(str(raw).replace("%", ""))
    except ValueError:
        return None


def fmt_plot_number(value):
    if value is None:
        return ""
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text if text else "0"


def labeled_visible_phases(phases):
    labels = []
    counts = defaultdict(int)
    for phase in phases:
        name = phase["name"]
        counts[name] += 1
        label = name if counts[name] == 1 else f"{name} {counts[name]}"
        labeled = dict(phase)
        labeled["label"] = label
        labels.append(labeled)
    return labels


def phase_at_time(phases, x):
    if not phases:
        return "", None
    for idx, phase in enumerate(phases):
        start = phase["start"]
        end = phase["end"]
        if start <= x < end or (idx == len(phases) - 1 and start <= x <= end):
            return phase.get("label", phase["name"]), x - start
    return "", None


def target_segments_from_summary(rows, warmup_offset, reference_phases=None):
    segments = []
    seen = set()
    offsets = summary_phase_offsets(rows, reference_phases)
    for row in rows:
        phase = row.get("phase", "")
        if not phase or phase.lower() == "warmup":
            continue
        try:
            raw_start = float(row.get("phase_start_s", ""))
            raw_end = float(row.get("phase_end_s", ""))
        except ValueError:
            continue
        offset_key = row.get("run_id", "") or row.get("transport", "")
        offset = offsets.get(offset_key, 0.0)
        start = raw_start - offset
        end = raw_end - offset
        key = (phase, start, end)
        if key in seen:
            continue
        target = optional_float(row, "delivery_rate")
        if target is None:
            continue
        segments.append({
            "start": max(0.0, start - warmup_offset),
            "end": end - warmup_offset,
            "target": target,
        })
        seen.add(key)
    return segments


def target_at_time(segments, x):
    for idx, segment in enumerate(segments):
        if segment["start"] <= x < segment["end"] or (idx == len(segments) - 1 and segment["start"] <= x <= segment["end"]):
            return segment["target"]
    return None


def run_ids_by_transport(rows):
    run_ids = {}
    for row in rows:
        transport = row.get("transport", "")
        run_id = row.get("run_id", "")
        if transport and run_id and transport not in run_ids:
            run_ids[transport] = run_id
    return run_ids


def plot_points_inputs(rows, summary_path):
    yield os.path.abspath(__file__)
    if summary_path:
        yield summary_path
    for art_dir in transport_artifacts(rows, summary_path):
        if not os.path.isdir(art_dir):
            continue
        for name in os.listdir(art_dir):
            if name.endswith(".csv") or name == "EARLY_STOP.json":
                yield os.path.join(art_dir, name)


def plot_points_needs_rebuild(path, summary_path, rows, force=False):
    if force or not os.path.exists(path):
        return True
    cache_mtime = os.path.getmtime(path)
    for input_path in plot_points_inputs(rows, summary_path):
        if os.path.exists(input_path) and os.path.getmtime(input_path) > cache_mtime:
            return True
    return False


def forward_fill_plot_columns(rows_by_key, columns):
    keys_by_transport = defaultdict(list)
    for transport, x in rows_by_key:
        keys_by_transport[transport].append((transport, x))

    for transport, keys in keys_by_transport.items():
        keys = sorted(keys, key=lambda item: item[1])
        last = {}
        for key in keys:
            row = rows_by_key[key]
            for column in columns:
                value = row.get(column, "")
                if value != "":
                    last[column] = value
                elif column in last:
                    row[column] = last[column]

        next_values = {}
        for key in reversed(keys):
            row = rows_by_key[key]
            for column in columns:
                value = row.get(column, "")
                if value != "":
                    next_values[column] = value
                elif column in next_values:
                    row[column] = next_values[column]


def write_plot_points_csv(path, summary_rows, visible_phases, warmup_offset, series_map, reference_phases=None):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    labeled_phases = labeled_visible_phases(visible_phases)
    target_segments = target_segments_from_summary(summary_rows, warmup_offset, reference_phases)
    run_ids = run_ids_by_transport(summary_rows)
    rows_by_key = {}

    metric_specs = [
        ("delivery_throughput_msg_s", series_map["throughput"], 1.0),
        ("p50_latency_ms", series_map["p50"], 1.0),
        ("p95_latency_ms", series_map["p95"], 1.0),
        ("p99_latency_ms", series_map["p99"], 1.0),
        ("avg_latency_ms", series_map["avg_latency"], 1.0),
        ("cpu_cores", series_map["cpu"], 1.0),
        ("memory_gb", series_map["mem"], 1.0),
        ("network_rx_gbps", series_map["rx"], 1.0 / 1_000_000_000.0),
        ("network_tx_gbps", series_map["tx"], 1.0 / 1_000_000_000.0),
    ]

    for column, series, scale in metric_specs:
        for transport, points in series.items():
            for x, value in points:
                key = (transport, round(float(x), 6))
                if key not in rows_by_key:
                    phase, phase_time = phase_at_time(labeled_phases, x)
                    rows_by_key[key] = {
                        "transport": transport,
                        "run_id": run_ids.get(transport, ""),
                        "time_s": fmt_plot_number(x),
                        "phase": phase,
                        "phase_time_s": fmt_plot_number(phase_time),
                        "target_throughput_msg_s": fmt_plot_number(target_at_time(target_segments, x)),
                    }
                rows_by_key[key][column] = fmt_plot_number(value * scale)

    forward_fill_plot_columns(rows_by_key, ["cpu_cores", "memory_gb", "network_rx_gbps", "network_tx_gbps"])

    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLOT_POINT_FIELDS)
        writer.writeheader()
        for transport, x in sorted(rows_by_key, key=lambda item: (item[1], item[0])):
            row = {field: rows_by_key[(transport, x)].get(field, "") for field in PLOT_POINT_FIELDS}
            writer.writerow(row)


def load_plot_points_csv(path):
    series_map = {
        "throughput": defaultdict(list),
        "p50": defaultdict(list),
        "p95": defaultdict(list),
        "p99": defaultdict(list),
        "avg_latency": defaultdict(list),
        "cpu": defaultdict(list),
        "mem": defaultdict(list),
        "rx": defaultdict(list),
        "tx": defaultdict(list),
    }
    metric_specs = [
        ("delivery_throughput_msg_s", "throughput", 1.0),
        ("p50_latency_ms", "p50", 1.0),
        ("p95_latency_ms", "p95", 1.0),
        ("p99_latency_ms", "p99", 1.0),
        ("avg_latency_ms", "avg_latency", 1.0),
        ("cpu_cores", "cpu", 1.0),
        ("memory_gb", "mem", 1.0),
        ("network_rx_gbps", "rx", 1_000_000_000.0),
        ("network_tx_gbps", "tx", 1_000_000_000.0),
    ]
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))

    for row in rows:
        transport = row.get("transport", "")
        x = optional_float(row, "time_s")
        if not transport or x is None:
            continue
        for column, key, scale in metric_specs:
            value = optional_float(row, column)
            if value is not None:
                series_map[key][transport].append((x, value * scale))
    return series_map


def offset_points(points, offset, trim_before=None):
    selected = points
    if trim_before is not None:
        selected = [(x, y) for x, y in selected if x >= trim_before]
    if not offset:
        return selected
    return [(x + offset, y) for x, y in selected]


def artifact_time_offsets(rows, summary_path=None):
    offsets = {}
    summary_raw_dir = os.path.dirname(os.path.abspath(summary_path)) if summary_path else ""
    for row in rows:
        art_dir = row.get("artifacts_dir", "")
        if not art_dir:
            continue
        start = optional_float(row, "phase_start_s")
        if start is None:
            continue
        resolved = resolve_artifacts_dir(art_dir, row.get("run_id", ""), summary_raw_dir)
        if resolved not in offsets or start < offsets[resolved]:
            offsets[resolved] = start
    return offsets


def raw_plot_series_from_artifacts(rows, summary_path=None, reference_phases=None):
    series_map = {
        "throughput": defaultdict(list),
        "p50": defaultdict(list),
        "p95": defaultdict(list),
        "p99": defaultdict(list),
        "avg_latency": defaultdict(list),
        "cpu": defaultdict(list),
        "mem": defaultdict(list),
        "rx": defaultdict(list),
        "tx": defaultdict(list),
    }
    alignments = artifact_alignments(rows, summary_path, reference_phases)
    for art_dir, transport in transport_artifacts(rows, summary_path).items():
        run_start = first_active_pub_ts(art_dir)
        if run_start <= 0:
            continue
        alignment = alignments.get(art_dir, {"shift": 0.0, "trim_before": None})
        shift = alignment.get("shift", 0.0)
        trim_before = alignment.get("trim_before")
        sub_throughput, sub_p50, sub_p95, sub_p99, sub_avg_latency = time_series_from_sub(art_dir, run_start)
        stat_cpu, stat_mem, stat_rx, stat_tx = time_series_from_stats(art_dir, run_start)
        series_map["throughput"][transport].extend(offset_points(sub_throughput, shift, trim_before))
        series_map["p50"][transport].extend(offset_points(sub_p50, shift, trim_before))
        series_map["p95"][transport].extend(offset_points(sub_p95, shift, trim_before))
        series_map["p99"][transport].extend(offset_points(sub_p99, shift, trim_before))
        series_map["avg_latency"][transport].extend(offset_points(sub_avg_latency, shift, trim_before))
        series_map["cpu"][transport].extend(offset_points(stat_cpu, shift, trim_before))
        series_map["mem"][transport].extend(offset_points(stat_mem, shift, trim_before))
        series_map["rx"][transport].extend(offset_points(stat_rx, shift, trim_before))
        series_map["tx"][transport].extend(offset_points(stat_tx, shift, trim_before))
    return series_map


def normalize_plot_series(series_map, warmup_offset, visible_end):
    normalized = {}
    for key, series in series_map.items():
        shifted = shift_series_after(series, warmup_offset)
        trimmed = trim_series_until(shifted, visible_end)
        normalized[key] = bucket_duplicate_timestamps(trimmed)
    return normalized


def load_or_build_plot_series(args, rows, phases, visible_phases, warmup_offset, visible_end):
    bucket_seconds = max(1.0, float(args.bucket_seconds or 1.0))
    plot_points_path = args.plot_points or default_plot_points_path(args.summary, bucket_seconds)
    force_rebuild = args.rebuild_plot_points or args.latest_run_per_transport
    if plot_points_needs_rebuild(plot_points_path, args.summary, rows, force_rebuild):
        series_map = normalize_plot_series(raw_plot_series_from_artifacts(rows, args.summary, phases), warmup_offset, visible_end)
        series_map = bucket_series_map(series_map, bucket_seconds)
        write_plot_points_csv(plot_points_path, rows, visible_phases, warmup_offset, series_map, phases)
        if bucket_seconds > 1:
            print(f"[plot] Wrote {fmt_plot_number(bucket_seconds)}s-bucketed plot points: {plot_points_path}")
        else:
            print(f"[plot] Wrote plot points: {plot_points_path}")
    else:
        series_map = load_plot_points_csv(plot_points_path)
        print(f"[plot] Using plot points: {plot_points_path}")
    return series_map, plot_points_path


def rates_match(left, right):
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= max(1e-6, abs(float(right)) * 1e-6)


def run_alignment(run_phases, reference_phases):
    if not run_phases or not reference_phases:
        return {"shift": 0.0, "trim_before": None}
    for run_phase in run_phases:
        run_name = (run_phase.get("name") or "").strip().lower()
        if not is_burst_phase(run_name):
            continue
        for reference_phase in reference_phases:
            reference_name = (reference_phase.get("name") or "").strip().lower()
            if is_burst_phase(reference_name) and rates_match(run_phase.get("rate"), reference_phase.get("rate")):
                shift = reference_phase["start"] - run_phase["start"]
                trim_before = run_phase["start"] if abs(shift) > 1e-9 else None
                return {"shift": shift, "trim_before": trim_before}
    return {"shift": 0.0, "trim_before": None}


def run_alignment_shift(run_phases, reference_phases):
    return run_alignment(run_phases, reference_phases).get("shift", 0.0)


def summary_phase_offsets(rows, reference_phases=None):
    offsets = {}
    for key, group in grouped_summary_rows(rows).items():
        starts = [optional_float(row, "phase_start_s") for row in group]
        starts = [start for start in starts if start is not None]
        if not starts:
            continue
        shift = run_alignment_shift(normalized_phases_for_rows(group), reference_phases or [])
        offsets[key] = min(starts) - shift
    return offsets


def artifact_alignments(rows, summary_path=None, reference_phases=None):
    alignments = {}
    if not reference_phases:
        return alignments
    summary_raw_dir = os.path.dirname(os.path.abspath(summary_path)) if summary_path else ""
    grouped = defaultdict(list)
    for row in rows:
        art_dir = row.get("artifacts_dir", "")
        if not art_dir:
            continue
        resolved = resolve_artifacts_dir(art_dir, row.get("run_id", ""), summary_raw_dir)
        grouped[resolved].append(row)
    for art_dir, group in grouped.items():
        alignments[art_dir] = run_alignment(normalized_phases_for_rows(group), reference_phases)
    return alignments


def phase_matches_reference(phase, start, end, rate, reference_phases):
    if not reference_phases:
        return True
    lower = (phase or "").strip().lower()
    for reference_phase in reference_phases:
        ref_lower = (reference_phase.get("name") or "").strip().lower()
        if lower != ref_lower:
            continue
        if not rates_match(rate, reference_phase.get("rate")):
            continue
        if abs(start - reference_phase["start"]) <= 1e-6 and abs(end - reference_phase["end"]) <= 1e-6:
            return True
    return False


def normalized_summary_phase_key(row, transport, phase, start, end, offsets):
    key = row.get("run_id", "") or transport
    offset = offsets.get(key, 0.0)
    return (phase, start - offset, end - offset)


def save_phase_plot(out_dir, filename, title, ylabel, rows, metric, ext, dpi, legend=False, log_y=False, y_scale=1.0, reference_phases=None):
    phases = []
    labels = []
    label_counts = defaultdict(int)
    transports = []
    values = defaultdict(dict)
    phase_offsets = summary_phase_offsets(rows, reference_phases)
    for row in rows:
        phase = row.get("phase", "")
        transport = row.get("transport", "")
        if not phase or not transport:
            continue
        if phase == "warmup":
            continue
        try:
            start = float(row.get("phase_start_s", ""))
            end = float(row.get("phase_end_s", ""))
        except ValueError:
            continue
        phase_key = normalized_summary_phase_key(row, transport, phase, start, end, phase_offsets)
        rate = optional_float(row, "rate_per_pub")
        if not phase_matches_reference(phase, phase_key[1], phase_key[2], rate, reference_phases):
            continue
        if phase_key not in phases:
            phases.append(phase_key)
            label_counts[phase] += 1
            labels.append(phase if label_counts[phase] == 1 else f"{phase} {label_counts[phase]}")
        value = optional_float(row, metric)
        if value is None:
            continue
        if transport not in transports:
            transports.append(transport)
        values[transport][phase_key] = value
    if not phases or not transports:
        return None
    latex = ext == ".pdf"
    fig, ax = plt.subplots(figsize=LATEX_FIGSIZE if ext == ".pdf" else (9, 5))
    xs = list(range(len(phases)))
    for transport in sorted(transports):
        ys = [
            (values[transport][phase] / y_scale) if phase in values[transport] else math.nan
            for phase in phases
        ]
        marker, color = transport_style(transport)
        ax.plot(
            xs,
            ys,
            marker=marker,
            linestyle="-",
            markersize=PLOT_MARKER_SIZE,
            markevery=1,
            linewidth=PLOT_LINEWIDTH,
            color=color,
            label=display_transport_label(transport),
        )
    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
        ax.set_yscale("symlog", linthresh=1.0)
    anchor_axes_at_zero(ax, x_max=max(xs) if xs else None)
    if legend:
        ax.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=min(4, max(1, len(transports))),
            frameon=False,
            fontsize=PLOT_LEGEND_FONTSIZE,
            handlelength=PLOT_LEGEND_HANDLE_LENGTH,
            columnspacing=PLOT_LEGEND_COLUMN_SPACING,
            handletextpad=PLOT_LEGEND_HANDLE_TEXT_PAD,
        )
    fig.tight_layout()
    gallery_file = save_fig(fig, out_dir, filename, ext, dpi)
    plt.close(fig)
    return gallery_file



def _positive_float(row, key):
    raw = row.get(key, "")
    if raw is None or raw == "":
        return None
    try:
        value = float(str(raw).replace("%", ""))
    except ValueError:
        return None
    return value if value > 0 else None


def save_latency_whisker_plot(out_dir, filename, title, rows, ext, dpi, legend=False, log_y=True, reference_phases=None):
    phases = []
    labels = []
    label_counts = defaultdict(int)
    transports = []
    values = defaultdict(dict)
    warned_missing_quartiles = False
    phase_offsets = summary_phase_offsets(rows, reference_phases)

    for row in rows:
        phase = row.get("phase", "")
        transport = row.get("transport", "")
        if not phase or not transport or phase == "warmup":
            continue
        try:
            start = float(row.get("phase_start_s", ""))
            end = float(row.get("phase_end_s", ""))
        except ValueError:
            continue

        q1_ms = _positive_float(row, "p25_ms")
        p50_ms = _positive_float(row, "p50_ms")
        q3_ms = _positive_float(row, "p75_ms")
        min_ms = _positive_float(row, "min_ms")
        max_ms = _positive_float(row, "max_ms")
        p99_ms = _positive_float(row, "p99_ms")
        if p50_ms is None:
            continue
        if q1_ms is None or q3_ms is None:
            warned_missing_quartiles = True
            continue

        q1_ms = min(q1_ms, p50_ms)
        q3_ms = max(q3_ms, p50_ms)
        whislo = min_ms if min_ms is not None else q1_ms
        whishi = max_ms if max_ms is not None else (p99_ms if p99_ms is not None else q3_ms)
        whislo = min(whislo, q1_ms)
        whishi = max(whishi, q3_ms)

        phase_key = normalized_summary_phase_key(row, transport, phase, start, end, phase_offsets)
        rate = optional_float(row, "rate_per_pub")
        if not phase_matches_reference(phase, phase_key[1], phase_key[2], rate, reference_phases):
            continue
        if phase_key not in phases:
            phases.append(phase_key)
            label_counts[phase] += 1
            labels.append(phase if label_counts[phase] == 1 else f"{phase} {label_counts[phase]}")
        if transport not in transports:
            transports.append(transport)
        values[transport][phase_key] = {
            "q1": q1_ms,
            "med": p50_ms,
            "q3": q3_ms,
            "whislo": whislo,
            "whishi": whishi,
        }

    if warned_missing_quartiles:
        print("[plot] Warning: skipped phase latency boxplot rows without p25_ms/p75_ms; regenerate summary_by_phase.csv for quartile boxes")
    if not phases or not transports:
        return None

    latex = ext == ".pdf"
    fig, ax = plt.subplots(figsize=LATEX_FIGSIZE if ext == ".pdf" else (10, 5.5))
    xs = list(range(len(phases)))
    transports = sorted(transports)
    group_width = 0.82
    slot_width = group_width / max(1, len(transports))
    box_width = min(slot_width * 0.62, 0.08)

    for idx, transport in enumerate(transports):
        _marker, color = transport_style(transport)
        offset = -group_width / 2.0 + slot_width * (idx + 0.5)
        bxp_stats = []
        positions = []
        for phase_index, phase in enumerate(phases):
            latency = values[transport].get(phase)
            if not latency:
                continue
            bxp_stats.append({
                "label": "",
                "q1": latency["q1"],
                "med": latency["med"],
                "q3": latency["q3"],
                "whislo": latency["whislo"],
                "whishi": latency["whishi"],
                "fliers": [],
            })
            positions.append(xs[phase_index] + offset)
        if not bxp_stats:
            continue

        bp = ax.bxp(
            bxp_stats,
            positions=positions,
            widths=box_width,
            patch_artist=True,
            showfliers=False,
            manage_ticks=False,
        )
        for patch in bp["boxes"]:
            patch.set_facecolor(color)
            patch.set_edgecolor(color)
            patch.set_alpha(0.32)
            patch.set_linewidth(0.9)
        for median in bp["medians"]:
            median.set_color(color)
            median.set_linewidth(1.4)
        for whisker in bp["whiskers"]:
            whisker.set_color(color)
            whisker.set_linewidth(1.0)
            whisker.set_alpha(0.85)
        for cap in bp["caps"]:
            cap.set_color(color)
            cap.set_linewidth(1.0)
            cap.set_alpha(0.9)

    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Latency quartile boxplot (ms)")
    if len(labels) > 6:
        ax.tick_params(axis="x", rotation=20)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, axis="y", alpha=0.3)
    if log_y:
        ax.set_yscale("symlog", linthresh=1.0)
    anchor_axes_at_zero(ax, x_max=max(xs) if xs else None)
    if legend:
        handles = []
        legend_labels = []
        for transport in transports:
            _marker, color = transport_style(transport)
            handles.append(Line2D([0], [0], color=color, marker="s", linestyle="-", linewidth=1.2, markersize=PLOT_MARKER_SIZE, markerfacecolor=color, alpha=0.7))
            legend_labels.append(display_transport_label(transport))
        ax.legend(
            handles,
            legend_labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=min(4, max(1, len(transports))),
            frameon=False,
            fontsize=PLOT_LEGEND_FONTSIZE,
            handlelength=PLOT_LEGEND_HANDLE_LENGTH,
            columnspacing=PLOT_LEGEND_COLUMN_SPACING,
            handletextpad=PLOT_LEGEND_HANDLE_TEXT_PAD,
        )
    fig.tight_layout()
    gallery_file = save_fig(fig, out_dir, filename, ext, dpi)
    plt.close(fig)
    return gallery_file

def write_gallery(out_dir, summary, images, plot_points=None):
    path = os.path.join(out_dir, "README.md")
    with open(path, "w") as f:
        f.write("# Bursty Fan-Out Plots\n\n")
        f.write(f"- Summary: `{os.path.abspath(summary)}`\n")
        if plot_points:
            f.write(f"- Plot points: `{os.path.abspath(plot_points)}`\n")
        f.write("\n")
        for title, filename in images:
            if filename:
                f.write(f"## {title}\n\n")
                f.write(f"![{title}]({filename})\n\n")


def main() -> int:
    args = parse_args()
    if not args.out_dir:
        summary_dir = os.path.dirname(os.path.abspath(args.summary))
        bench_dir = os.path.dirname(summary_dir)
        args.out_dir = os.path.join(bench_dir, "plots")
        print(f"[plot] No --out-dir provided, using default: {args.out_dir}")
    os.makedirs(args.out_dir, exist_ok=True)
    plot_ext, plot_dpi = configure_plot_style(args.latex)
    if args.latex:
        print(f"[plot] LaTeX mode enabled: PDF + PNG output, {plot_dpi} DPI")
    inline_legend = args.inline_legend
    rows = read_summary(args.summary)
    if args.latest_run_per_transport:
        original_count = len(rows)
        rows = filter_latest_run_per_transport(rows)
        print(f"[plot] Latest-run filtering kept {len(rows)} of {original_count} summary rows")
    phases = complete_phases_from_profile(rows, args.profile)
    warmup_offset = warmup_end(phases)
    visible_phases = phases_with_compact_labels(phases_after_warmup(phases))
    failures = early_stop_markers(rows, args.summary, warmup_offset)
    failed_labels = set(failures)

    common_window = args.window if args.window is not None else None
    common_marker_every = args.marker_every if args.marker_every is not None else None
    artifacts = transport_artifacts(rows, args.summary)
    visible_end = max((phase["end"] for phase in visible_phases), default=0.0)
    series_map, plot_points_path = load_or_build_plot_series(args, rows, phases, visible_phases, warmup_offset, visible_end)
    plot_start_offset = start_offset_for_phase(visible_phases, args.start_phase)
    if plot_start_offset > 0:
        visible_phases = rebase_phases_from(visible_phases, plot_start_offset)
        series_map = rebase_series_map_from(series_map, plot_start_offset)
        failures = rebase_failure_markers_from(failures, plot_start_offset)
        print(f"[plot] Starting time-series plots at {args.start_phase} ({fmt_plot_number(plot_start_offset)}s after warmup)")

    throughput = series_map["throughput"]
    p50 = series_map["p50"]
    p95 = series_map["p95"]
    p99 = series_map["p99"]
    avg_latency = series_map["avg_latency"]
    cpu = series_map["cpu"]
    mem = series_map["mem"]
    rx = series_map["rx"]
    tx = series_map["tx"]
    target_throughput = target_delivery_series(rows, warmup_offset, visible_phases)

    latency_probe = representative_series(p99, p95, p50, avg_latency)
    resource_probe = representative_series(tx, rx, cpu, mem)
    throughput_window = choose_smoothing_window("throughput", throughput, visible_phases, args.throughput_window, common_window)
    latency_window = choose_smoothing_window("latency", latency_probe, visible_phases, args.latency_window, common_window)
    resource_window = choose_smoothing_window("resource", resource_probe, visible_phases, args.resource_window, common_window)
    if args.bucket_seconds and args.bucket_seconds > 1 and common_window is None:
        if args.throughput_window is None:
            throughput_window = 1
        if args.latency_window is None:
            latency_window = 1
        if args.resource_window is None:
            resource_window = 1

    throughput = rolling_average_series(throughput, throughput_window)
    p50 = rolling_average_series(p50, latency_window)
    p95 = rolling_average_series(p95, latency_window)
    p99 = rolling_average_series(p99, latency_window)
    avg_latency = rolling_average_series(avg_latency, latency_window)
    cpu = rolling_average_series(cpu, resource_window)
    mem = rolling_average_series(mem, resource_window)
    rx = rolling_average_series(rx, resource_window)
    tx = rolling_average_series(tx, resource_window)

    latency_probe = representative_series(p99, p95, p50, avg_latency)
    resource_probe = representative_series(tx, rx, cpu, mem)
    throughput_marker_every = choose_marker_every("throughput", throughput, args.throughput_marker_every, common_marker_every)
    latency_marker_every = choose_marker_every("latency", latency_probe, args.latency_marker_every, common_marker_every)
    resource_marker_every = choose_marker_every("resource", resource_probe, args.resource_marker_every, common_marker_every)
    print(
        "[plot] Auto/selected plot tuning: "
        f"bucket_seconds={fmt_plot_number(max(1.0, float(args.bucket_seconds or 1.0)))}, "
        f"throughput window={throughput_window}, marker_every={throughput_marker_every}; "
        f"latency window={latency_window}, marker_every={latency_marker_every}; "
        f"resource window={resource_window}, marker_every={resource_marker_every}"
    )

    images = []
    legend_file = standalone_legend(args.out_dir, artifacts.values(), plot_ext, plot_dpi, include_target=bool(target_throughput))
    if legend_file:
        images.append(("Legend", legend_file))
    images.append(("Delivery Throughput vs Time", save_line_plot(args.out_dir, "delivery_throughput_vs_time.png", "Delivery Throughput vs Time", args.throughput_y_label, throughput, visible_phases, plot_ext, plot_dpi, inline_legend, y_scale=args.throughput_y_scale, target_series=target_throughput, marker_every=throughput_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("P99 Latency vs Time", save_line_plot(args.out_dir, "p99_latency_vs_time.png", "P99 Latency vs Time", "P99 latency (ms)", p99, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("P95 Latency vs Time", save_line_plot(args.out_dir, "p95_latency_vs_time.png", "P95 Latency vs Time", "P95 latency (ms)", p95, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("P50 Latency vs Time", save_line_plot(args.out_dir, "p50_latency_vs_time.png", "P50 Latency vs Time", "P50 latency (ms)", p50, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("Average Latency vs Time", save_line_plot(args.out_dir, "avg_latency_vs_time.png", "Average Latency vs Time", "Average latency (ms)", avg_latency, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("CPU vs Time", save_line_plot(args.out_dir, "cpu_vs_time.png", "CPU Utilization vs Time", "CPU Core Used", cpu, visible_phases, plot_ext, plot_dpi, inline_legend, marker_every=resource_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("Memory vs Time", save_line_plot(args.out_dir, "memory_vs_time.png", "Memory Utilization vs Time", "Memory (GB)", mem, visible_phases, plot_ext, plot_dpi, inline_legend, marker_every=resource_marker_every, step=True, y_min=0.0, compact_phase_labels=True, failure_markers=failures)))
    images.append(("Network TX vs Time", save_line_plot(args.out_dir, "network_tx_vs_time.png", "Network TX vs Time", "Network bandwidth (Gbps)", tx, visible_phases, plot_ext, plot_dpi, inline_legend, y_scale=1_000_000_000.0, marker_every=resource_marker_every, compact_phase_labels=True, failure_markers=failures)))
    images.append(("Latency Quartile Boxplot by Phase", save_latency_whisker_plot(args.out_dir, "phase_latency_whisker.png", "Latency Quartile Boxplot by Phase", rows, plot_ext, plot_dpi, inline_legend, reference_phases=phases)))
    images.append(("Phase P99 Latency", save_phase_plot(args.out_dir, "phase_p99_latency.png", "P99 Latency by Phase", "P99 latency (ms)", rows, "p99_ms", plot_ext, plot_dpi, inline_legend, log_y=True, reference_phases=phases)))
    images.append(("Phase Average Latency", save_phase_plot(args.out_dir, "phase_avg_latency.png", "Average Latency by Phase", "Average latency (ms)", rows, "avg_latency_ms", plot_ext, plot_dpi, inline_legend, log_y=True, reference_phases=phases)))
    images.append(("Phase Delivery Throughput", save_phase_plot(args.out_dir, "phase_delivery_throughput.png", "Delivery Throughput by Phase", "Messages/s (x10$^5$)", rows, "sub_tps", plot_ext, plot_dpi, inline_legend, y_scale=100000.0, reference_phases=phases)))
    images.append(("Phase CPU", save_phase_plot(args.out_dir, "phase_cpu.png", "CPU by Phase", "CPU Core Used", rows, "max_cpu_perc", plot_ext, plot_dpi, inline_legend, y_scale=100.0, reference_phases=phases)))
    images.append(("Phase Memory", save_phase_plot(args.out_dir, "phase_memory.png", "Memory by Phase", "Memory (GB)", rows, "max_mem_used_bytes", plot_ext, plot_dpi, inline_legend, y_scale=1024.0 ** 3, reference_phases=phases)))
    write_gallery(args.out_dir, args.summary, images, plot_points_path)
    print(f"[plot] Wrote plots to {args.out_dir}")
    print(f"[plot] Wrote gallery: {os.path.join(args.out_dir, 'README.md')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
