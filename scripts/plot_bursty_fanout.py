#!/usr/bin/env python3
"""Plot bursty fan-out phase summaries and raw time series."""
import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

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
BURST_SHADE_COLOR = "#f3d8a2"
BURST_SHADE_ALPHA = 0.32


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True, help="Path to summary_by_phase.csv")
    p.add_argument(
        "--out-dir",
        help="Output directory for plots (default: plots directory in the benchmark folder)",
    )
    p.add_argument("--profile", default=DEFAULT_PROFILE)
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


def read_summary(path: str):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def transport_artifacts(rows):
    seen = {}
    for row in rows:
        art = row.get("artifacts_dir", "")
        transport = row.get("transport", "")
        if art and transport and art not in seen:
            seen[art] = transport
    return seen


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




def phases_from_summary(rows):
    phases = []
    seen = set()
    for row in rows:
        name = row.get("phase", "")
        if not name:
            continue
        try:
            start = float(row.get("phase_start_s", ""))
            end = float(row.get("phase_end_s", ""))
            rate = float(row.get("rate_per_pub", ""))
        except ValueError:
            continue
        key = (name, start, end)
        if key in seen:
            continue
        phases.append({"name": name, "start": start, "end": end, "rate": rate})
        seen.add(key)
    return phases


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


def extend_missing_series_with_zero(series, end):
    if end <= 0:
        return series
    extended = defaultdict(list)
    for label, points in series.items():
        points = sorted(points, key=lambda point: point[0])
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


def extend_missing_series_with_last(series, end):
    if end <= 0:
        return series
    extended = defaultdict(list)
    for label, points in series.items():
        points = sorted(points, key=lambda point: point[0])
        if not points:
            continue
        extended[label] = list(points)
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
        legend_labels.append(label)
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

def target_delivery_series(rows, warmup_offset):
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


def add_phase_lines(ax, phases, latex=False, show_boundaries=True):
    if not phases:
        return
    for phase in phases:
        if phase["name"].lower() == "burst":
            ax.axvspan(
                phase["start"],
                phase["end"],
                color=BURST_SHADE_COLOR,
                alpha=BURST_SHADE_ALPHA,
                linewidth=0,
                zorder=1,
            )
        if show_boundaries:
            ax.axvline(phase["start"], color="#999999", linewidth=0.8, linestyle="--", alpha=0.6)
        mid = (phase["start"] + phase["end"]) / 2.0
        ax.text(
            mid,
            1.02,
            phase["name"],
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=12,
            color=ax.xaxis.label.get_color(),
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.7, "pad": 1.5},
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


def save_line_plot(out_dir, filename, title, ylabel, series, phases, ext, dpi, legend=False, log_y=False, y_scale=1.0, target_series=None, marker_every=None, step=False, y_min=None):
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
        plot_points = marker_sampled_points(points, marker_every)
        marker, color = transport_style(label)
        ax.plot(
            [p[0] for p in plot_points],
            [p[1] / y_scale for p in plot_points],
            marker=marker if marker_every != 0 else None,
            linestyle="-",
            markersize=PLOT_MARKER_SIZE,
            linewidth=2.2 if has_target else PLOT_LINEWIDTH,
            color=color,
            label=label,
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
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(ylabel)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
        if series_has_nonpositive(series, y_scale):
            ax.set_yscale("symlog", linthresh=1.0)
        else:
            ax.set_yscale("log")
    add_phase_lines(ax, phases, latex, show_boundaries=not has_target)
    if y_min is not None:
        ax.set_ylim(bottom=y_min)
    if has_target:
        if phases:
            x_min = min(phase["start"] for phase in phases)
            x_max = max(phase["end"] for phase in phases)
        elif x_values:
            x_min = min(x_values)
            x_max = max(x_values)
        else:
            x_min = x_max = None
        if x_min is not None and x_max > x_min:
            margin = 0.01 * (x_max - x_min)
            ax.set_xlim(x_min - margin, x_max + margin)
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
    fig.tight_layout()
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


def default_plot_points_path(summary_path):
    return os.path.join(os.path.dirname(os.path.abspath(summary_path)), PLOT_POINTS_FILENAME)


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


def target_segments_from_summary(rows, warmup_offset):
    segments = []
    seen = set()
    for row in rows:
        phase = row.get("phase", "")
        if not phase or phase.lower() == "warmup":
            continue
        try:
            raw_start = float(row.get("phase_start_s", ""))
            raw_end = float(row.get("phase_end_s", ""))
        except ValueError:
            continue
        key = (phase, raw_start, raw_end)
        if key in seen:
            continue
        target = optional_float(row, "delivery_rate")
        if target is None:
            continue
        segments.append({
            "start": max(0.0, raw_start - warmup_offset),
            "end": raw_end - warmup_offset,
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
    for art_dir in transport_artifacts(rows):
        if not os.path.isdir(art_dir):
            continue
        for name in os.listdir(art_dir):
            if name.endswith(".csv"):
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


def write_plot_points_csv(path, summary_rows, visible_phases, warmup_offset, series_map):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    labeled_phases = labeled_visible_phases(visible_phases)
    target_segments = target_segments_from_summary(summary_rows, warmup_offset)
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
        for row in csv.DictReader(handle):
            transport = row.get("transport", "")
            x = optional_float(row, "time_s")
            if not transport or x is None:
                continue
            for column, key, scale in metric_specs:
                value = optional_float(row, column)
                if value is not None:
                    series_map[key][transport].append((x, value * scale))
    return series_map


def raw_plot_series_from_artifacts(rows):
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
    for art_dir, transport in transport_artifacts(rows).items():
        run_start = first_active_pub_ts(art_dir)
        if run_start <= 0:
            continue
        sub_throughput, sub_p50, sub_p95, sub_p99, sub_avg_latency = time_series_from_sub(art_dir, run_start)
        stat_cpu, stat_mem, stat_rx, stat_tx = time_series_from_stats(art_dir, run_start)
        series_map["throughput"][transport].extend(sub_throughput)
        series_map["p50"][transport].extend(sub_p50)
        series_map["p95"][transport].extend(sub_p95)
        series_map["p99"][transport].extend(sub_p99)
        series_map["avg_latency"][transport].extend(sub_avg_latency)
        series_map["cpu"][transport].extend(stat_cpu)
        series_map["mem"][transport].extend(stat_mem)
        series_map["rx"][transport].extend(stat_rx)
        series_map["tx"][transport].extend(stat_tx)
    return series_map


def normalize_plot_series(series_map, warmup_offset, visible_end):
    normalized = {}
    for key, series in series_map.items():
        shifted = shift_series_after(series, warmup_offset)
        trimmed = trim_series_until(shifted, visible_end)
        normalized[key] = bucket_duplicate_timestamps(trimmed)
    return normalized


def load_or_build_plot_series(args, rows, visible_phases, warmup_offset, visible_end):
    plot_points_path = args.plot_points or default_plot_points_path(args.summary)
    if plot_points_needs_rebuild(plot_points_path, args.summary, rows, args.rebuild_plot_points):
        series_map = normalize_plot_series(raw_plot_series_from_artifacts(rows), warmup_offset, visible_end)
        write_plot_points_csv(plot_points_path, rows, visible_phases, warmup_offset, series_map)
        print(f"[plot] Wrote plot points: {plot_points_path}")
    else:
        series_map = load_plot_points_csv(plot_points_path)
        print(f"[plot] Using plot points: {plot_points_path}")
    return series_map, plot_points_path


def save_phase_plot(out_dir, filename, title, ylabel, rows, metric, ext, dpi, legend=False, log_y=False, y_scale=1.0):
    phases = []
    labels = []
    label_counts = defaultdict(int)
    transports = []
    values = defaultdict(dict)
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
        phase_key = (phase, start, end)
        if phase_key not in phases:
            phases.append(phase_key)
            label_counts[phase] += 1
            labels.append(phase if label_counts[phase] == 1 else f"{phase} {label_counts[phase]}")
        if transport not in transports:
            transports.append(transport)
        values[transport][phase_key] = fnum(row, metric, 0.0)
    if not phases or not transports:
        return None
    latex = ext == ".pdf"
    fig, ax = plt.subplots(figsize=LATEX_FIGSIZE if ext == ".pdf" else (9, 5))
    xs = list(range(len(phases)))
    for transport in sorted(transports):
        ys = [values[transport].get(phase, 0.0) / y_scale for phase in phases]
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
            label=transport,
        )
    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
        if any((values[transport].get(phase, 0.0) / y_scale) <= 0 for transport in transports for phase in phases):
            ax.set_yscale("symlog", linthresh=1.0)
        else:
            ax.set_yscale("log")
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


def save_latency_whisker_plot(out_dir, filename, title, rows, ext, dpi, legend=False, log_y=True):
    phases = []
    labels = []
    label_counts = defaultdict(int)
    transports = []
    values = defaultdict(dict)
    warned_missing_quartiles = False

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

        phase_key = (phase, start, end)
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
        ax.set_yscale("log")
    if legend:
        handles = []
        legend_labels = []
        for transport in transports:
            _marker, color = transport_style(transport)
            handles.append(Line2D([0], [0], color=color, marker="s", linestyle="-", linewidth=1.2, markersize=PLOT_MARKER_SIZE, markerfacecolor=color, alpha=0.7))
            legend_labels.append(transport)
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
    phases = phases_from_summary(rows) or parse_profile(args.profile)
    warmup_offset = warmup_end(phases)
    visible_phases = phases_after_warmup(phases)

    common_window = args.window if args.window is not None else None
    common_marker_every = args.marker_every if args.marker_every is not None else None
    artifacts = transport_artifacts(rows)
    visible_end = max((phase["end"] for phase in visible_phases), default=0.0)
    series_map, plot_points_path = load_or_build_plot_series(args, rows, visible_phases, warmup_offset, visible_end)

    throughput = series_map["throughput"]
    p50 = series_map["p50"]
    p95 = series_map["p95"]
    p99 = series_map["p99"]
    avg_latency = series_map["avg_latency"]
    cpu = series_map["cpu"]
    mem = series_map["mem"]
    rx = series_map["rx"]
    tx = series_map["tx"]
    target_throughput = target_delivery_series(rows, warmup_offset)

    latency_probe = representative_series(p99, p95, p50, avg_latency)
    resource_probe = representative_series(tx, rx, cpu, mem)
    throughput_window = choose_smoothing_window("throughput", throughput, visible_phases, args.throughput_window, common_window)
    latency_window = choose_smoothing_window("latency", latency_probe, visible_phases, args.latency_window, common_window)
    resource_window = choose_smoothing_window("resource", resource_probe, visible_phases, args.resource_window, common_window)

    throughput = rolling_average_series(throughput, throughput_window)
    throughput = extend_missing_series_with_zero(throughput, visible_end)
    p50 = rolling_average_series(p50, latency_window)
    p95 = rolling_average_series(p95, latency_window)
    p99 = rolling_average_series(p99, latency_window)
    avg_latency = rolling_average_series(avg_latency, latency_window)
    p50 = extend_missing_series_with_zero(p50, visible_end)
    p95 = extend_missing_series_with_zero(p95, visible_end)
    p99 = extend_missing_series_with_zero(p99, visible_end)
    avg_latency = extend_missing_series_with_zero(avg_latency, visible_end)
    cpu = rolling_average_series(cpu, resource_window)
    mem = rolling_average_series(mem, resource_window)
    rx = rolling_average_series(rx, resource_window)
    tx = rolling_average_series(tx, resource_window)
    cpu = extend_missing_series_with_zero(cpu, visible_end)
    mem = extend_missing_series_with_last(mem, visible_end)
    rx = extend_missing_series_with_zero(rx, visible_end)
    tx = extend_missing_series_with_zero(tx, visible_end)

    latency_probe = representative_series(p99, p95, p50, avg_latency)
    resource_probe = representative_series(tx, rx, cpu, mem)
    throughput_marker_every = choose_marker_every("throughput", throughput, args.throughput_marker_every, common_marker_every)
    latency_marker_every = choose_marker_every("latency", latency_probe, args.latency_marker_every, common_marker_every)
    resource_marker_every = choose_marker_every("resource", resource_probe, args.resource_marker_every, common_marker_every)
    print(
        "[plot] Auto/selected plot tuning: "
        f"throughput window={throughput_window}, marker_every={throughput_marker_every}; "
        f"latency window={latency_window}, marker_every={latency_marker_every}; "
        f"resource window={resource_window}, marker_every={resource_marker_every}"
    )

    images = []
    legend_file = standalone_legend(args.out_dir, artifacts.values(), plot_ext, plot_dpi, include_target=bool(target_throughput))
    if legend_file:
        images.append(("Legend", legend_file))
    images.append(("Delivery Throughput vs Time", save_line_plot(args.out_dir, "delivery_throughput_vs_time.png", "Delivery Throughput vs Time", "Throughput\n(x10,000 msg/s)", throughput, visible_phases, plot_ext, plot_dpi, inline_legend, y_scale=10_000.0, target_series=target_throughput, marker_every=throughput_marker_every)))
    images.append(("P99 Latency vs Time", save_line_plot(args.out_dir, "p99_latency_vs_time.png", "P99 Latency vs Time", "P99 latency (ms)", p99, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every)))
    images.append(("P95 Latency vs Time", save_line_plot(args.out_dir, "p95_latency_vs_time.png", "P95 Latency vs Time", "P95 latency (ms)", p95, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every)))
    images.append(("P50 Latency vs Time", save_line_plot(args.out_dir, "p50_latency_vs_time.png", "P50 Latency vs Time", "P50 latency (ms)", p50, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every)))
    images.append(("Average Latency vs Time", save_line_plot(args.out_dir, "avg_latency_vs_time.png", "Average Latency vs Time", "Average latency (ms)", avg_latency, visible_phases, plot_ext, plot_dpi, inline_legend, log_y=True, marker_every=latency_marker_every)))
    images.append(("CPU vs Time", save_line_plot(args.out_dir, "cpu_vs_time.png", "CPU Utilization vs Time", "CPU Core Used", cpu, visible_phases, plot_ext, plot_dpi, inline_legend, marker_every=resource_marker_every)))
    images.append(("Memory vs Time", save_line_plot(args.out_dir, "memory_vs_time.png", "Memory Utilization vs Time", "Memory (GB)", mem, visible_phases, plot_ext, plot_dpi, inline_legend, marker_every=resource_marker_every, step=True, y_min=0.0)))
    images.append(("Network TX vs Time", save_line_plot(args.out_dir, "network_tx_vs_time.png", "Network TX vs Time", "Network bandwidth (Gbps)", tx, visible_phases, plot_ext, plot_dpi, inline_legend, y_scale=1_000_000_000.0, marker_every=resource_marker_every)))
    images.append(("Latency Quartile Boxplot by Phase", save_latency_whisker_plot(args.out_dir, "phase_latency_whisker.png", "Latency Quartile Boxplot by Phase", rows, plot_ext, plot_dpi, inline_legend)))
    images.append(("Phase P99 Latency", save_phase_plot(args.out_dir, "phase_p99_latency.png", "P99 Latency by Phase", "P99 latency (ms)", rows, "p99_ms", plot_ext, plot_dpi, inline_legend, log_y=True)))
    images.append(("Phase Average Latency", save_phase_plot(args.out_dir, "phase_avg_latency.png", "Average Latency by Phase", "Average latency (ms)", rows, "avg_latency_ms", plot_ext, plot_dpi, inline_legend, log_y=True)))
    images.append(("Phase Delivery Throughput", save_phase_plot(args.out_dir, "phase_delivery_throughput.png", "Delivery Throughput by Phase", "Messages/s (x10$^5$)", rows, "sub_tps", plot_ext, plot_dpi, inline_legend, y_scale=100000.0)))
    images.append(("Phase CPU", save_phase_plot(args.out_dir, "phase_cpu.png", "CPU by Phase", "CPU Core Used", rows, "max_cpu_perc", plot_ext, plot_dpi, inline_legend, y_scale=100.0)))
    images.append(("Phase Memory", save_phase_plot(args.out_dir, "phase_memory.png", "Memory by Phase", "Memory (GB)", rows, "max_mem_used_bytes", plot_ext, plot_dpi, inline_legend, y_scale=1024.0 ** 3)))
    write_gallery(args.out_dir, args.summary, images, plot_points_path)
    print(f"[plot] Wrote plots to {args.out_dir}")
    print(f"[plot] Wrote gallery: {os.path.join(args.out_dir, 'README.md')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
