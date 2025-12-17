#!/usr/bin/env python3
"""
Bar chart comparing latency across transports for selected payload sizes.
Usage:
  python3 scripts/plot_latency_bars.py --summary results/.../summary.csv --out-dir plots/
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Payload sizes to include (bytes)
PAYLOADS = [1024, 65536, 1048576]  # 1KB, 64KB, 1MB
PAYLOAD_LABELS = {1024: "1 KB", 65536: "64 KB", 1048576: "1 MB"}

# Display names for transports
TRANSPORT_NAMES = {
    "zenoh": "Zenoh",
    "nats": "NATS",
    "redis": "Redis",
    "mqtt_mosquitto": "Mosquitto",
    "mqtt_emqx": "EMQX",
    "mqtt_hivemq": "HiveMQ",
    "mqtt_rabbitmq": "RabbitMQ",
    "mqtt_artemis": "Artemis",
}

# Colors for each payload size
COLORS = {1024: "#2ecc71", 65536: "#3498db", 1048576: "#e74c3c"}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True, help="Path to summary.csv")
    p.add_argument("--out-dir", help="Output directory for plots (default: 'plots' sibling to summary.csv)")
    args = p.parse_args()
    
    # Default out-dir to 'plots' directory next to summary.csv
    if not args.out_dir:
        summary_path = Path(args.summary)
        args.out_dir = summary_path.parent.parent / "plots"
    
    return args


def plot_percentile(df, transports, percentile, out_dir):
    """Generate bar chart for a single percentile."""
    latency_col = f"{percentile}_ms"
    
    # Prepare data
    x = np.arange(len(transports))
    width = 0.25  # Width of bars
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    for i, payload in enumerate(PAYLOADS):
        payload_df = df[df["payload"] == payload]
        
        # Get latency for each transport (NaN if missing)
        latencies = []
        for t in transports:
            t_df = payload_df[payload_df["transport"] == t]
            if len(t_df) > 0:
                latencies.append(t_df[latency_col].values[0])
            else:
                latencies.append(np.nan)
        
        offset = (i - 1) * width
        bars = ax.bar(x + offset, latencies, width, 
                      label=PAYLOAD_LABELS[payload],
                      color=COLORS[payload],
                      edgecolor="black", linewidth=0.5)
        
        # Add value labels on bars
        for bar, val in zip(bars, latencies):
            if not np.isnan(val):
                height = bar.get_height()
                # Format based on magnitude
                if val >= 100:
                    label = f"{val:.0f}"
                elif val >= 10:
                    label = f"{val:.1f}"
                else:
                    label = f"{val:.2f}"
                ax.annotate(label,
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha="center", va="bottom", fontsize=8, rotation=45)
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], rotation=30, ha="right")
    
    # Labels and title
    percentile_name = percentile.upper()
    ax.set_ylabel(f"Latency ({percentile_name}) [ms]", fontsize=12)
    ax.set_xlabel("Transport / Broker", fontsize=12)
    ax.set_title(f"Message Latency by Payload Size ({percentile_name})", fontsize=14)
    
    # Use log scale if range is large
    max_val = df[latency_col].max()
    min_val = df[latency_col].min()
    if max_val / min_val > 100:
        ax.set_yscale("log")
        ax.set_ylabel(f"Latency ({percentile_name}) [ms] (log scale)", fontsize=12)
    
    ax.legend(title="Payload Size", loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    
    plt.tight_layout()
    
    out_path = out_dir / f"latency_bars_{percentile}.png"
    plt.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")
    
    # Also save PDF
    out_path_pdf = out_dir / f"latency_bars_{percentile}.pdf"
    plt.savefig(out_path_pdf)
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.summary)
    
    # Filter to selected payloads
    df = df[df["payload"].isin(PAYLOADS)]
    
    # Filter out failed runs (sub_tps < 10% of rate)
    df = df[df["sub_tps"] >= df["rate"] * 0.1]
    
    # Get unique transports in order
    transports = df["transport"].unique()
    # Sort by display name
    transports = sorted(transports, key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    # Generate charts for p50 and p99
    for percentile in ["p50", "p99"]:
        plot_percentile(df, transports, percentile, out_dir)
    
    # Generate combined P50 + P99 chart
    plot_combined_p50_p99(df, transports, out_dir)


def plot_combined_p50_p99(df, transports, out_dir):
    """Generate bar chart with P50 bars and P99 whiskers."""
    
    x = np.arange(len(transports))
    width = 0.25  # Width of bars
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    for i, payload in enumerate(PAYLOADS):
        payload_df = df[df["payload"] == payload]
        
        # Get P50 and P99 latencies for each transport
        p50_vals = []
        p99_vals = []
        for t in transports:
            t_df = payload_df[payload_df["transport"] == t]
            if len(t_df) > 0:
                p50_vals.append(t_df["p50_ms"].values[0])
                p99_vals.append(t_df["p99_ms"].values[0])
            else:
                p50_vals.append(np.nan)
                p99_vals.append(np.nan)
        
        p50_arr = np.array(p50_vals)
        p99_arr = np.array(p99_vals)
        
        # Error bars: from P50 up to P99
        yerr_lower = np.zeros_like(p50_arr)
        yerr_upper = p99_arr - p50_arr
        yerr_upper = np.where(np.isnan(yerr_upper), 0, yerr_upper)
        
        offset = (i - 1) * width
        bars = ax.bar(x + offset, p50_arr, width, 
                      label=PAYLOAD_LABELS[payload],
                      color=COLORS[payload],
                      edgecolor="black", linewidth=0.5)
        
        # Add error bars (P99 whiskers)
        ax.errorbar(x + offset, p50_arr, yerr=[yerr_lower, yerr_upper],
                    fmt='none', ecolor='black', capsize=3, capthick=1.5, elinewidth=1.5)
        
        # Add P50 value labels on bars
        for j, (bar, p50, p99) in enumerate(zip(bars, p50_vals, p99_vals)):
            if not np.isnan(p50):
                height = bar.get_height()
                # Format P50
                if p50 >= 100:
                    label = f"{p50:.0f}"
                elif p50 >= 10:
                    label = f"{p50:.1f}"
                else:
                    label = f"{p50:.2f}"
                ax.annotate(label,
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, -12),
                            textcoords="offset points",
                            ha="center", va="top", fontsize=7, color="white", fontweight="bold")
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], rotation=30, ha="right")
    
    # Labels and title
    ax.set_ylabel("Latency [ms]", fontsize=12)
    ax.set_xlabel("Transport / Broker", fontsize=12)
    ax.set_title("Message Latency by Payload Size (Bar=P50, Whisker=P99)", fontsize=14)
    
    # Use log scale if range is large
    max_val = df["p99_ms"].max()
    min_val = df["p50_ms"].min()
    if max_val / min_val > 100:
        ax.set_yscale("log")
        ax.set_ylabel("Latency [ms] (log scale)", fontsize=12)
    
    ax.legend(title="Payload Size", loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_bars_p50_p99_combined.png"
    plt.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_bars_p50_p99_combined.pdf"
    plt.savefig(out_path_pdf)
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


if __name__ == "__main__":
    main()
