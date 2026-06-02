#!/usr/bin/env python3
"""Plot bursty fan-out phase summaries and raw time series."""
import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
PLOT_MARK_EVERY = 10
PLOT_LEGEND_FONTSIZE = 5.5
PLOT_LEGEND_HANDLE_LENGTH = 0.9
PLOT_LEGEND_COLUMN_SPACING = 0.45
PLOT_LEGEND_HANDLE_TEXT_PAD = 0.25
BURST_SHADE_COLOR = "#f6c26b"
BURST_SHADE_ALPHA = 0.28


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True, help="Path to summary_by_phase.csv")
    p.add_argument(
        "--out-dir",
        help="Output directory for plots (default: plots directory in the benchmark folder)",
    )
    p.add_argument("--profile", default=DEFAULT_PROFILE)
    p.add_argument("--legend", action="store_true")
    p.add_argument(
        "--latex",
        action="store_true",
        help="Write LaTeX-ready PDF plots and PNG siblings",
    )
    p.add_argument(
        "--window",
        type=int,
        default=None,
        help="Rolling-average window for all time-series plots; category-specific windows override it",
    )
    p.add_argument(
        "--throughput-window",
        type=int,
        default=None,
        help="Rolling-average window for delivery throughput plots; use 1 to disable",
    )
    p.add_argument(
        "--latency-window",
        type=int,
        default=None,
        help="Rolling-average window for latency plots; use 1 to disable",
    )
    p.add_argument(
        "--resource-window",
        type=int,
        default=None,
        help="Rolling-average window for CPU/memory/network plots; use 1 to disable",
    )
    p.add_argument(
        "--marker-every",
        type=int,
        default=PLOT_MARK_EVERY,
        help="Draw a marker every N points on time-series plots; use 0 to hide markers",
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
        if not name or name in seen:
            continue
        try:
            start = float(row.get("phase_start_s", ""))
            end = float(row.get("phase_end_s", ""))
            rate = float(row.get("rate_per_pub", ""))
        except ValueError:
            continue
        phases.append({"name": name, "start": start, "end": end, "rate": rate})
        seen.add(name)
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
        points = sorted(points)
        values = [y for _x, y in points]
        for idx, (x, _y) in enumerate(points):
            start = max(0, idx - half_left)
            end = min(len(values), idx + half_right + 1)
            window_values = values[start:end]
            smoothed[label].append((x, sum(window_values) / len(window_values)))
    return smoothed


def target_delivery_series(rows, warmup_offset):
    points = []
    seen = set()
    for row in rows:
        phase = row.get("phase", "")
        if not phase or phase.lower() == "warmup" or phase in seen:
            continue
        try:
            start = float(row.get("phase_start_s", "")) - warmup_offset
            end = float(row.get("phase_end_s", "")) - warmup_offset
            target = float(row.get("delivery_rate", ""))
        except ValueError:
            continue
        if end <= 0:
            continue
        points.extend([(max(0.0, start), target), (end, target)])
        seen.add(phase)
    return {"Fan-out target": points} if points else {}


def add_phase_lines(ax, phases, latex=False):
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
                zorder=0,
            )
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
    ax.axvline(phases[-1]["end"], color="#999999", linewidth=0.8, linestyle="--", alpha=0.6)


def save_line_plot(out_dir, filename, title, ylabel, series, phases, ext, dpi, legend=False, log_y=False, y_scale=1.0, target_series=None, marker_every=PLOT_MARK_EVERY):
    if not series:
        return None
    latex = ext == ".pdf"
    fig, ax = plt.subplots(figsize=LATEX_FIGSIZE if ext == ".pdf" else (10, 5.5))
    for label, points in sorted(series.items()):
        points = sorted(points)
        if not points:
            continue
        xs = [p[0] for p in points]
        ys = [p[1] / y_scale for p in points]
        marker, color = transport_style(label)
        ax.plot(
            xs,
            ys,
            marker=marker if marker_every != 0 else None,
            linestyle="-",
            markersize=PLOT_MARKER_SIZE,
            markevery=marker_every if marker_every > 0 else None,
            linewidth=PLOT_LINEWIDTH,
            color=color,
            label=label,
        )
    if target_series:
        for label, points in sorted(target_series.items()):
            if not points:
                continue
            xs = [p[0] for p in points]
            ys = [p[1] / y_scale for p in points]
            ax.step(
                xs,
                ys,
                where="post",
                linestyle="--",
                linewidth=1.2 if latex else 1.5,
                color="#444444",
                alpha=0.8,
                label=label,
            )
    ax.set_xlabel("Time after warmup (s)")
    ax.set_ylabel(ylabel)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
        ax.set_yscale("log")
    add_phase_lines(ax, phases, latex)
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


def save_phase_plot(out_dir, filename, title, ylabel, rows, metric, ext, dpi, legend=False, log_y=False, y_scale=1.0):
    phases = []
    transports = []
    values = defaultdict(dict)
    for row in rows:
        phase = row.get("phase", "")
        transport = row.get("transport", "")
        if not phase or not transport:
            continue
        if phase == "warmup":
            continue
        if phase not in phases:
            phases.append(phase)
        if transport not in transports:
            transports.append(transport)
        values[transport][phase] = fnum(row, metric, 0.0)
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
    ax.set_xticklabels(phases)
    ax.set_ylabel(ylabel)
    if latex:
        ax.grid(False)
    else:
        ax.grid(True, alpha=0.3)
    if log_y:
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


def write_gallery(out_dir, summary, images):
    path = os.path.join(out_dir, "README.md")
    with open(path, "w") as f:
        f.write("# Bursty Fan-Out Plots\n\n")
        f.write(f"- Summary: `{os.path.abspath(summary)}`\n\n")
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
    rows = read_summary(args.summary)
    phases = phases_from_summary(rows) or parse_profile(args.profile)
    warmup_offset = warmup_end(phases)
    visible_phases = phases_after_warmup(phases)

    common_window = args.window if args.window is not None else None
    throughput_window = args.throughput_window if args.throughput_window is not None else (common_window if common_window is not None else 5)
    latency_window = args.latency_window if args.latency_window is not None else (common_window if common_window is not None else 1)
    resource_window = args.resource_window if args.resource_window is not None else (common_window if common_window is not None else 7)
    artifacts = transport_artifacts(rows)

    throughput = defaultdict(list)
    p50 = defaultdict(list)
    p95 = defaultdict(list)
    p99 = defaultdict(list)
    avg_latency = defaultdict(list)
    cpu = defaultdict(list)
    mem = defaultdict(list)
    rx = defaultdict(list)
    tx = defaultdict(list)

    for art_dir, transport in artifacts.items():
        run_start = first_active_pub_ts(art_dir)
        if run_start <= 0:
            continue
        sub_throughput, sub_p50, sub_p95, sub_p99, sub_avg_latency = time_series_from_sub(art_dir, run_start)
        stat_cpu, stat_mem, stat_rx, stat_tx = time_series_from_stats(art_dir, run_start)
        throughput[transport].extend(sub_throughput)
        p50[transport].extend(sub_p50)
        p95[transport].extend(sub_p95)
        p99[transport].extend(sub_p99)
        avg_latency[transport].extend(sub_avg_latency)
        cpu[transport].extend(stat_cpu)
        mem[transport].extend(stat_mem)
        rx[transport].extend(stat_rx)
        tx[transport].extend(stat_tx)

    visible_end = max((phase["end"] for phase in visible_phases), default=0.0)
    throughput = trim_series_until(shift_series_after(throughput, warmup_offset), visible_end)
    throughput = bucket_duplicate_timestamps(throughput)
    throughput = rolling_average_series(throughput, throughput_window)
    target_throughput = target_delivery_series(rows, warmup_offset)
    p50 = trim_series_until(shift_series_after(p50, warmup_offset), visible_end)
    p95 = trim_series_until(shift_series_after(p95, warmup_offset), visible_end)
    p99 = trim_series_until(shift_series_after(p99, warmup_offset), visible_end)
    avg_latency = trim_series_until(shift_series_after(avg_latency, warmup_offset), visible_end)
    p50 = rolling_average_series(p50, latency_window)
    p95 = rolling_average_series(p95, latency_window)
    p99 = rolling_average_series(p99, latency_window)
    avg_latency = rolling_average_series(avg_latency, latency_window)
    cpu = trim_series_until(shift_series_after(cpu, warmup_offset), visible_end)
    mem = trim_series_until(shift_series_after(mem, warmup_offset), visible_end)
    rx = trim_series_until(shift_series_after(rx, warmup_offset), visible_end)
    tx = trim_series_until(shift_series_after(tx, warmup_offset), visible_end)
    cpu = rolling_average_series(cpu, resource_window)
    mem = rolling_average_series(mem, resource_window)
    rx = rolling_average_series(rx, resource_window)
    tx = rolling_average_series(tx, resource_window)

    images = []
    images.append(("Delivery Throughput vs Time", save_line_plot(args.out_dir, "delivery_throughput_vs_time.png", "Delivery Throughput vs Time", "Messages/s (x10$^5$)", throughput, visible_phases, plot_ext, plot_dpi, args.legend, y_scale=100000.0, target_series=target_throughput, marker_every=args.marker_every)))
    images.append(("P99 Latency vs Time", save_line_plot(args.out_dir, "p99_latency_vs_time.png", "P99 Latency vs Time", "P99 latency (ms)", p99, visible_phases, plot_ext, plot_dpi, args.legend, log_y=True, marker_every=args.marker_every)))
    images.append(("P95 Latency vs Time", save_line_plot(args.out_dir, "p95_latency_vs_time.png", "P95 Latency vs Time", "P95 latency (ms)", p95, visible_phases, plot_ext, plot_dpi, args.legend, log_y=True, marker_every=args.marker_every)))
    images.append(("P50 Latency vs Time", save_line_plot(args.out_dir, "p50_latency_vs_time.png", "P50 Latency vs Time", "P50 latency (ms)", p50, visible_phases, plot_ext, plot_dpi, args.legend, log_y=True, marker_every=args.marker_every)))
    images.append(("Average Latency vs Time", save_line_plot(args.out_dir, "avg_latency_vs_time.png", "Average Latency vs Time", "Average latency (ms)", avg_latency, visible_phases, plot_ext, plot_dpi, args.legend, log_y=True, marker_every=args.marker_every)))
    images.append(("CPU vs Time", save_line_plot(args.out_dir, "cpu_vs_time.png", "CPU Utilization vs Time", "CPU Core Used", cpu, visible_phases, plot_ext, plot_dpi, args.legend, marker_every=args.marker_every)))
    images.append(("Memory vs Time", save_line_plot(args.out_dir, "memory_vs_time.png", "Memory Utilization vs Time", "Memory (GB)", mem, visible_phases, plot_ext, plot_dpi, args.legend, marker_every=args.marker_every)))
    images.append(("Network TX vs Time", save_line_plot(args.out_dir, "network_tx_vs_time.png", "Network TX vs Time", "Network bandwidth (Gbps)", tx, visible_phases, plot_ext, plot_dpi, args.legend, y_scale=1_000_000_000.0, marker_every=args.marker_every)))
    images.append(("Phase P99 Latency", save_phase_plot(args.out_dir, "phase_p99_latency.png", "P99 Latency by Phase", "P99 latency (ms)", rows, "p99_ms", plot_ext, plot_dpi, args.legend, log_y=True)))
    images.append(("Phase Average Latency", save_phase_plot(args.out_dir, "phase_avg_latency.png", "Average Latency by Phase", "Average latency (ms)", rows, "avg_latency_ms", plot_ext, plot_dpi, args.legend, log_y=True)))
    images.append(("Phase Delivery Throughput", save_phase_plot(args.out_dir, "phase_delivery_throughput.png", "Delivery Throughput by Phase", "Messages/s (x10$^5$)", rows, "sub_tps", plot_ext, plot_dpi, args.legend, y_scale=100000.0)))
    images.append(("Phase CPU", save_phase_plot(args.out_dir, "phase_cpu.png", "CPU by Phase", "CPU Core Used", rows, "max_cpu_perc", plot_ext, plot_dpi, args.legend, y_scale=100.0)))
    images.append(("Phase Memory", save_phase_plot(args.out_dir, "phase_memory.png", "Memory by Phase", "Memory (GB)", rows, "max_mem_used_bytes", plot_ext, plot_dpi, args.legend, y_scale=1024.0 ** 3)))
    write_gallery(args.out_dir, args.summary, images)
    print(f"[plot] Wrote plots to {args.out_dir}")
    print(f"[plot] Wrote gallery: {os.path.join(args.out_dir, 'README.md')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
