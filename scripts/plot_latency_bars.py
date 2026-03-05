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
PAYLOADS = [1024, 16384, 1048576]  # 1KB, 16KB, 1MB
PAYLOAD_LABELS = {1024: "1 KB", 16384: "16 KB", 1048576: "1 MB"}

# Display names for transports
TRANSPORT_NAMES = {
    "zenoh": "Zenoh",
    "nats": "NATS",
    "redis": "Redis",
    "mqtt_mosquitto": "Mosquitto",
    "mqtt_emqx": "EMQX",
    "mqtt_hivemq": "HiveMQ",
    "mqtt_rabbitmq": "MQTT-RabbitMQ",
    "mqtt_artemis": "Artemis",
    "amqp-rabbitmq": "AMQP-RabbitMQ",
}

# Colors for each transport (for separate payload charts)
TRANSPORT_COLORS = {
    "zenoh": "#3498db",
    "nats": "#2ecc71",
    "redis": "#e74c3c",
    "mqtt_mosquitto": "#9b59b6",
    "mqtt_emqx": "#f39c12",
    "mqtt_hivemq": "#1abc9c",
    "mqtt_rabbitmq": "#e67e22",
    "mqtt_artemis": "#34495e",
    "amqp-rabbitmq": "#d35400",
}

# Colors for each payload size (for combined charts)
PAYLOAD_COLORS = {1024: "#2ecc71", 16384: "#3498db", 1048576: "#e74c3c"}

# QoS color scheme (traffic light intuition)
QOS_COLORS = {
    0: '#4CAF50',  # Green - best effort (fastest)
    1: '#FFC107',  # Amber - at least once
    2: '#F44336',  # Red - exactly once (slowest)
}

QOS_LABELS = {
    0: 'QoS 0 (At Most Once)',
    1: 'QoS 1 (At Least Once)',
    2: 'QoS 2 (Exactly Once)',
}

# Base broker names for QoS comparison
MQTT_BROKER_NAMES = {
    "mosquitto": "Mosquitto",
    "emqx": "EMQX",
    "hivemq": "HiveMQ",
    "rabbitmq": "RabbitMQ",
    "artemis": "Artemis",
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--summary", required=True, help="Path to summary.csv")
    p.add_argument("--out-dir", help="Output directory for plots (default: 'bar-plot' in same directory as summary.csv)")
    p.add_argument("--qos", action="store_true", help="Generate QoS comparison chart instead of payload comparison")
    p.add_argument("--qos-data", action="store_true", help="Input CSV is QoS format (broker,qos columns) - generate all QoS plots")
    p.add_argument("--boxplot", action="store_true", help="Generate box plots for latency distribution")
    p.add_argument("--ci", action="store_true", help="Generate median latency with IQR error bars")
    p.add_argument("--avg", action="store_true", help="Generate average latency with stddev error bars")
    p.add_argument("--median-error", action="store_true", help="Generate median (P50) bar chart with min/P95 error bars")
    p.add_argument("--confidence", type=float, default=0.95, help="Confidence level for CI plots (default: 0.95 for 95%%)")
    p.add_argument("--payload", type=int, default=1024, help="Payload size to use for QoS comparison (default: 1024)")
    p.add_argument("--all-qos-plots", action="store_true", help="Generate all QoS plot types (avg+CI, boxplot, median+error)")
    args = p.parse_args()
    
    # Default out-dir to 'bar-plot' directory in the same folder as summary.csv
    if not args.out_dir:
        summary_path = Path(args.summary)
        args.out_dir = summary_path.parent / "bar-plot"
    
    return args


def extract_qos_info(transport):
    """
    Extract broker name and QoS level from transport string.
    e.g., 'mqtt_mosquitto_q2' -> ('mosquitto', 2)
         'mqtt_mosquitto_q2_failure' -> ('mosquitto', 2)
    Returns (None, None) if not a QoS-tagged transport.
    """
    import re
    # Match with optional suffix after _qN (e.g., _failure, _warmup, etc.)
    match = re.match(r'mqtt_(.+?)_q([012])(?:_|$)', transport)
    if match:
        return match.group(1), int(match.group(2))
    return None, None


def plot_per_payload(df, transports, out_dir):
    """Generate separate P50 bar charts for each payload size - publication ready."""
    
    # Use serif font for academic style - larger sizes for clarity
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    for payload in PAYLOADS:
        payload_df = df[df["payload"] == payload]
        payload_label = PAYLOAD_LABELS[payload]
        
        # Skip if no data for this payload
        if payload_df.empty:
            print(f"Skipping {payload_label}: no data")
            continue
        
        # Get P50 latencies for each transport
        p50_vals = []
        for t in transports:
            t_df = payload_df[payload_df["transport"] == t]
            if len(t_df) > 0:
                p50_vals.append(t_df["p50_ms"].values[0])
            else:
                p50_vals.append(np.nan)
        
        p50_arr = np.array(p50_vals)
        
        x = np.arange(len(transports))
        bar_width = 0.5  # Narrower bars
        
        fig, ax = plt.subplots(figsize=(8, 5))
        
        # Draw P50 bars - matplotlib default blue
        bars = ax.bar(x, p50_arr, width=bar_width, color='#1f77b4', edgecolor='#174a70', linewidth=0.8)
        
        # Add value labels above bars
        for bar, p50 in zip(bars, p50_vals):
            if not np.isnan(p50):
                height = bar.get_height()
                # Format based on magnitude
                if p50 >= 100:
                    p50_label = f"{p50:.0f}"
                elif p50 >= 10:
                    p50_label = f"{p50:.1f}"
                else:
                    p50_label = f"{p50:.2f}"
                ax.annotate(p50_label,
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha="center", va="bottom", fontsize=12, fontweight='medium')
        
        # X-axis labels
        ax.set_xticks(x)
        ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], rotation=45, ha="right")
        
        # Set y-axis limits
        max_p50 = np.nanmax(p50_arr)
        ax.set_ylim(0, max_p50 * 1.15)  # 15% headroom for labels
        
        # Labels - no title for academic papers (caption in LaTeX instead)
        ax.set_ylabel("Median Latency (P50) [ms]")
        
        # Clean academic styling - minimal grid
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
        ax.set_axisbelow(True)
        
        plt.tight_layout()
        
        # Save with payload in filename
        payload_suffix = f"{payload // 1024}KB" if payload < 1048576 else f"{payload // 1048576}MB"
        out_path = out_dir / f"latency_p50_{payload_suffix}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
        
        out_path_pdf = out_dir / f"latency_p50_{payload_suffix}.pdf"
        plt.savefig(out_path_pdf, bbox_inches='tight')
        print(f"Saved: {out_path_pdf}")
        
        plt.close()


def plot_per_payload_p50_p99(df, transports, out_dir):
    """Generate separate P50+P99 grouped bar charts for each payload size - publication ready."""
    from matplotlib.ticker import MaxNLocator
    
    # Use serif font for academic style - larger sizes for clarity
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Colors for P50 and P99
    P50_COLOR = '#1f77b4'  # Blue
    P99_COLOR = '#e57373'  # Light Red
    
    for payload in PAYLOADS:
        payload_df = df[df["payload"] == payload]
        payload_label = PAYLOAD_LABELS[payload]
        
        # Skip if no data for this payload
        if payload_df.empty:
            print(f"Skipping {payload_label}: no data")
            continue
        
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
        
        x = np.arange(len(transports))
        bar_width = 0.35  # Width for grouped bars
        
        # Check if we need a broken axis
        all_vals = np.concatenate([p50_arr[~np.isnan(p50_arr)], p99_arr[~np.isnan(p99_arr)]])
        max_val = np.nanmax(all_vals)
        
        # Find natural break point: if top 20% of values are > 2x the 80th percentile
        sorted_vals = np.sort(all_vals)
        p80 = np.percentile(sorted_vals, 80)
        top_vals = sorted_vals[sorted_vals > p80 * 1.5]
        
        use_broken_axis = len(top_vals) > 0 and max_val > p80 * 2
        
        if use_broken_axis:
            # Create broken axis figure
            fig, (ax_top, ax_bottom) = plt.subplots(2, 1, sharex=True, figsize=(10, 6),
                                                      gridspec_kw={'height_ratios': [1, 2], 'hspace': 0.05})
            
            # Determine break points
            break_low = p80 * 1.3   # Top of lower axis
            break_high = min(top_vals) * 0.9  # Bottom of upper axis
            
            # Draw bars on both axes
            for ax in [ax_top, ax_bottom]:
                ax.bar(x - bar_width/2, p50_arr, width=bar_width, 
                       label='P50 (Median)', color=P50_COLOR, edgecolor='#174a70', linewidth=0.8)
                ax.bar(x + bar_width/2, p99_arr, width=bar_width,
                       label='P99 (Tail)', color=P99_COLOR, edgecolor='#1e7b1e', linewidth=0.8)
            
            # Set axis limits
            ax_bottom.set_ylim(0, break_low)
            ax_top.set_ylim(break_high, max_val * 1.15)
            
            # Hide spines between axes
            ax_top.spines['bottom'].set_visible(False)
            ax_bottom.spines['top'].set_visible(False)
            ax_top.tick_params(bottom=False)
            
            # Add diagonal break marks
            d = 0.015  # Size of diagonal lines
            kwargs = dict(transform=ax_top.transAxes, color='k', clip_on=False, linewidth=1)
            ax_top.plot((-d, +d), (-d, +d), **kwargs)
            ax_top.plot((1-d, 1+d), (-d, +d), **kwargs)
            kwargs.update(transform=ax_bottom.transAxes)
            ax_bottom.plot((-d, +d), (1-d, 1+d), **kwargs)
            ax_bottom.plot((1-d, 1+d), (1-d, 1+d), **kwargs)
            
            # X-axis labels (only on bottom)
            ax_bottom.set_xticks(x)
            ax_bottom.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], rotation=45, ha="right")
            
            # Y-axis label spanning both
            fig.text(0.02, 0.5, 'Latency [ms]', va='center', rotation='vertical', fontsize=16)
            
            # Legend on top axis
            ax_top.legend(loc='upper right', bbox_to_anchor=(1.0, 1.2), ncol=2, framealpha=0.9)
            
            # Grid
            for ax in [ax_top, ax_bottom]:
                ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
                ax.set_axisbelow(True)
                ax.spines['right'].set_visible(False)
            ax_top.spines['top'].set_visible(False)
            
            ax = ax_bottom  # For tight_layout reference
        else:
            # Standard single axis chart
            fig, ax = plt.subplots(figsize=(10, 5))
            
            # Draw P50 bars (left)
            bars_p50 = ax.bar(x - bar_width/2, p50_arr, width=bar_width, 
                              label='P50 (Median)', color=P50_COLOR, edgecolor='#174a70', linewidth=0.8)
            
            # Draw P99 bars (right)
            bars_p99 = ax.bar(x + bar_width/2, p99_arr, width=bar_width,
                              label='P99 (Tail)', color=P99_COLOR, edgecolor='#c94c4c', linewidth=0.8)
            
            # X-axis labels
            ax.set_xticks(x)
            ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], rotation=45, ha="right")
            
            # Set y-axis limits and use whole numbers
            ax.set_ylim(0, max_val * 1.12)  # 12% headroom
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            
            # Labels - no title for academic papers (caption in LaTeX instead)
            ax.set_ylabel("Latency [ms]")
            
            # Legend - place outside plot area at top right
            ax.legend(loc='upper right', bbox_to_anchor=(1.0, 1.15), ncol=2, framealpha=0.9)
            
            # Clean academic styling - minimal grid
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
            ax.set_axisbelow(True)
            
            plt.tight_layout()
        
        # Save with payload in filename
        payload_suffix = f"{payload // 1024}KB" if payload < 1048576 else f"{payload // 1048576}MB"
        out_path = out_dir / f"latency_p50_p99_{payload_suffix}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
        
        out_path_pdf = out_dir / f"latency_p50_p99_{payload_suffix}.pdf"
        plt.savefig(out_path_pdf, bbox_inches='tight')
        print(f"Saved: {out_path_pdf}")
        
        plt.close()


def plot_qos_comparison(df, out_dir, payload=1024):
    """
    Generate bar chart comparing QoS levels across MQTT brokers.
    Groups bars by broker with QoS 0/1/2 as grouped bars.
    """
    # Use serif font for academic style
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Filter to specified payload
    df = df[df["payload"] == payload]
    
    # Extract QoS info and filter to QoS-tagged transports
    df = df.copy()
    df[["broker", "qos"]] = df["transport"].apply(
        lambda t: pd.Series(extract_qos_info(t))
    )
    df = df.dropna(subset=["broker", "qos"])
    df["qos"] = df["qos"].astype(int)
    
    if df.empty:
        print("No QoS-tagged data found in summary.csv")
        return
    
    # Get unique brokers, sorted by display name
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = [0, 1, 2]
    
    x = np.arange(len(brokers))
    n_qos = len(qos_levels)
    width = 0.12  # Narrower bars for P50/P99 pairs
    group_width = width * 2 * n_qos + 0.1  # Total width per broker group
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Darker shades for P99 bars
    QOS_COLORS_P99 = {
        0: '#2E7D32',  # Darker green
        1: '#F57F17',  # Darker amber
        2: '#C62828',  # Darker red
    }
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        # Get P50 and P99 latencies for each broker
        p50_vals = []
        p99_vals = []
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                p50_vals.append(broker_df["p50_ms"].values[0])
                p99_vals.append(broker_df["p99_ms"].values[0])
            else:
                p50_vals.append(np.nan)
                p99_vals.append(np.nan)
        
        p50_arr = np.array(p50_vals)
        p99_arr = np.array(p99_vals)
        
        # Position offset for this QoS level (P50 and P99 side by side)
        base_offset = (i - 1) * (width * 2 + 0.04)  # Gap between QoS groups
        p50_offset = base_offset - width / 2
        p99_offset = base_offset + width / 2
        
        # P50 bars (original color)
        bars_p50 = ax.bar(x + p50_offset, p50_arr, width,
                          label=f"QoS {qos} P50" if i == 0 else f"QoS {qos} P50",
                          color=QOS_COLORS[qos],
                          edgecolor="black", linewidth=0.5)
        
        # P99 bars (darker shade)
        bars_p99 = ax.bar(x + p99_offset, p99_arr, width,
                          label=f"QoS {qos} P99" if i == 0 else f"QoS {qos} P99",
                          color=QOS_COLORS_P99[qos],
                          edgecolor="black", linewidth=0.5)
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    # Labels
    ax.set_ylabel("Latency [ms]")
    ax.set_xlabel("MQTT Broker")
    
    # Use log scale if range is large
    valid_p99 = df["p99_ms"].dropna()
    valid_p50 = df["p50_ms"].dropna()
    if len(valid_p99) > 0 and len(valid_p50) > 0:
        max_val = valid_p99.max()
        min_val = valid_p50.min()
        if min_val > 0 and max_val / min_val > 50:
            ax.set_yscale("log")
            ax.set_ylabel("Latency [ms] (log scale)")
    
    # Clean academic styling
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Custom legend: show QoS colors and P50/P99 distinction
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=QOS_COLORS[0], edgecolor='black', label='QoS 0'),
        Patch(facecolor=QOS_COLORS[1], edgecolor='black', label='QoS 1'),
        Patch(facecolor=QOS_COLORS[2], edgecolor='black', label='QoS 2'),
        Patch(facecolor='#9E9E9E', edgecolor='black', label='P50 (light)'),
        Patch(facecolor='#424242', edgecolor='black', label='P99 (dark)'),
    ]
    ax.legend(handles=legend_elements, loc="upper right", ncol=1, bbox_to_anchor=(1.15, 1.0))
    
    # Save - use fig.savefig for consistent output between PNG and PDF
    payload_suffix = f"{payload // 1024}KB" if payload < 1048576 else f"{payload // 1048576}MB"
    out_path = out_dir / f"latency_qos_comparison_{payload_suffix}.png"
    fig.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / f"latency_qos_comparison_{payload_suffix}.pdf"
    fig.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_loss_comparison(df, out_dir, payload=1024):
    """
    Generate bar chart comparing message loss percentage across MQTT brokers by QoS level.
    """
    # Use serif font for academic style
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Filter to specified payload
    df = df[df["payload"] == payload]
    
    # Check if loss_pct column exists
    if "loss_pct" not in df.columns:
        print("No loss_pct column found in summary.csv")
        return
    
    # Extract QoS info and filter to QoS-tagged transports
    df = df.copy()
    df[["broker", "qos"]] = df["transport"].apply(
        lambda t: pd.Series(extract_qos_info(t))
    )
    df = df.dropna(subset=["broker", "qos"])
    df["qos"] = df["qos"].astype(int)
    
    if df.empty:
        print("No QoS-tagged data found in summary.csv")
        return
    
    # Get unique brokers, sorted by display name
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = [0, 1, 2]
    
    x = np.arange(len(brokers))
    width = 0.25  # Width of bars
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        # Get loss_pct for each broker
        loss_vals = []
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                loss_vals.append(broker_df["loss_pct"].values[0])
            else:
                loss_vals.append(np.nan)
        
        loss_arr = np.array(loss_vals)
        
        offset = (i - 1) * width
        bars = ax.bar(x + offset, loss_arr, width,
                      label=QOS_LABELS[qos],
                      color=QOS_COLORS[qos],
                      edgecolor="black", linewidth=0.5)
        
        # Add value labels on bars
        for bar, loss in zip(bars, loss_vals):
            if not np.isnan(loss):
                height = bar.get_height()
                label = f"{loss:.1f}%"
                ax.annotate(label,
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha="center", va="bottom", fontsize=9, fontweight='medium')
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    # Labels
    ax.set_ylabel("Message Loss [%]")
    ax.set_xlabel("MQTT Broker")
    
    # Set y-axis to start at 0
    ax.set_ylim(bottom=0)
    
    # Clean academic styling
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    ax.legend(title="QoS Level", loc="upper right", bbox_to_anchor=(1.18, 1.0))
    
    plt.tight_layout()
    
    # Save
    payload_suffix = f"{payload // 1024}KB" if payload < 1048576 else f"{payload // 1048576}MB"
    out_path = out_dir / f"loss_qos_comparison_{payload_suffix}.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / f"loss_qos_comparison_{payload_suffix}.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_boxplots(df, out_dir):
    """
    Generate box plots for latency distribution per payload size.
    Uses p25, p50, p75, min, max from summary.csv to construct box plots.
    """
    # Use serif font for academic style
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Get unique payloads in the data
    payloads = sorted(df["payload"].unique())
    
    for payload in payloads:
        payload_df = df[df["payload"] == payload]
        
        if payload_df.empty:
            continue
        
        # Get transports sorted by display name
        transports = sorted(payload_df["transport"].unique(), 
                          key=lambda t: TRANSPORT_NAMES.get(t, t))
        
        fig, ax = plt.subplots(figsize=(max(8, len(transports) * 1.2), 6))
        
        # Build box plot data from summary statistics
        # matplotlib's bxp() can draw box plots from precomputed stats
        bxp_stats = []
        positions = []
        
        for i, transport in enumerate(transports):
            t_df = payload_df[payload_df["transport"] == transport]
            if len(t_df) == 0:
                continue
            
            row = t_df.iloc[0]
            
            # Check required columns exist
            required = ['p25_ms', 'p50_ms', 'p75_ms']
            if not all(col in row.index for col in required):
                print(f"Warning: Missing columns for box plot. Required: {required}")
                continue
            
            # Use P95 for upper whisker (cuts off extreme outliers)
            whislo = row.get('min_ms', row['p25_ms'])
            whishi = row.get('p95_ms', row.get('max_ms', row['p75_ms']))
            
            # Build stats dict for bxp()
            stats = {
                'med': row['p50_ms'],      # Median (Q2)
                'q1': row['p25_ms'],       # First quartile (Q1)
                'q3': row['p75_ms'],       # Third quartile (Q3)
                'whislo': whislo,          # Lower whisker
                'whishi': whishi,          # Upper whisker (P95)
                'fliers': [],              # No outliers (we have aggregated data)
                'label': TRANSPORT_NAMES.get(transport, transport),
            }
            bxp_stats.append(stats)
            positions.append(i)
        
        if not bxp_stats:
            print(f"No valid data for payload {payload}")
            plt.close()
            continue
        
        # Draw box plots
        bp = ax.bxp(bxp_stats, positions=positions, patch_artist=True,
                    widths=0.6, showfliers=False)
        
        # Color the boxes by transport
        for i, (patch, transport) in enumerate(zip(bp['boxes'], transports)):
            color = TRANSPORT_COLORS.get(transport, '#1f77b4')
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
            patch.set_edgecolor('black')
            patch.set_linewidth(1)
        
        # Style median lines
        for median in bp['medians']:
            median.set_color('black')
            median.set_linewidth(2)
        
        # Style whiskers and caps
        for whisker in bp['whiskers']:
            whisker.set_color('black')
            whisker.set_linewidth(1.2)
        for cap in bp['caps']:
            cap.set_color('black')
            cap.set_linewidth(1.2)
        
        # X-axis labels
        ax.set_xticks(positions)
        ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], 
                          rotation=45, ha="right")
        
        # Y-axis label
        ax.set_ylabel("Latency [ms]")
        
        # Clean academic styling
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
        ax.set_axisbelow(True)
        
        # Set y-axis to start at 0
        ax.set_ylim(bottom=0)
        
        plt.tight_layout()
        
        # Save
        if payload >= 1048576:
            payload_suffix = f"{payload // 1048576}MB"
        elif payload >= 1024:
            payload_suffix = f"{payload // 1024}KB"
        else:
            payload_suffix = f"{payload}B"
        
        out_path = out_dir / f"latency_boxplot_{payload_suffix}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
        
        out_path_pdf = out_dir / f"latency_boxplot_{payload_suffix}.pdf"
        plt.savefig(out_path_pdf, bbox_inches='tight')
        print(f"Saved: {out_path_pdf}")
        
        plt.close()


def plot_boxplots_grouped_by_transport(df, out_dir, payload_filter=None, suffix=""):
    """
    Generate box plots grouped by transport with payload sizes side by side.
    X-axis: transports, with grouped boxes for each payload size.
    
    Args:
        df: DataFrame with latency data
        out_dir: Output directory
        payload_filter: List of payload sizes to include (None = all)
        suffix: Suffix for output filename
    """
    # Use serif font for academic style - larger sizes
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 16,
        'axes.labelsize': 18,
        'axes.titlesize': 20,
        'xtick.labelsize': 15,
        'ytick.labelsize': 15,
        'legend.fontsize': 14,
        'legend.title_fontsize': 15,
    })
    
    # Filter to specified payloads
    if payload_filter is not None:
        df = df[df["payload"].isin(payload_filter)]
    
    # Get unique payloads and transports
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(), 
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    # Box width and spacing
    box_width = 0.25
    group_spacing = 0.15  # Extra space between transport groups
    
    fig, ax = plt.subplots(figsize=(max(10, n_transports * 1.5), 6))
    
    all_bxp_stats = []
    all_positions = []
    all_colors = []
    
    for t_idx, transport in enumerate(transports):
        # Base position for this transport group
        base_pos = t_idx * (n_payloads * box_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            if len(t_df) == 0:
                continue
            
            row = t_df.iloc[0]
            
            # Check required columns exist
            required = ['p25_ms', 'p50_ms', 'p75_ms']
            if not all(col in row.index for col in required):
                continue
            
            # Use P95 for upper whisker (cuts off extreme outliers)
            # Use min for lower whisker
            whislo = row.get('min_ms', row['p25_ms'])
            whishi = row.get('p95_ms', row.get('max_ms', row['p75_ms']))
            
            # Build stats dict for bxp()
            stats = {
                'med': row['p50_ms'],
                'q1': row['p25_ms'],
                'q3': row['p75_ms'],
                'whislo': whislo,
                'whishi': whishi,
                'fliers': [],
            }
            all_bxp_stats.append(stats)
            
            # Position within the group
            pos = base_pos + p_idx * (box_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
    
    if not all_bxp_stats:
        print("No valid data for grouped box plot")
        plt.close()
        return
    
    # Draw all box plots
    bp = ax.bxp(all_bxp_stats, positions=all_positions, patch_artist=True,
                widths=box_width, showfliers=False)
    
    # Color the boxes by payload
    for patch, color in zip(bp['boxes'], all_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.85)
        patch.set_edgecolor('black')
        patch.set_linewidth(1.5)
    
    # Style median lines - white for visibility on colored boxes
    for median in bp['medians']:
        median.set_color('white')
        median.set_linewidth(2.5)
    
    # Style whiskers and caps - darker
    for whisker in bp['whiskers']:
        whisker.set_color('black')
        whisker.set_linewidth(1.5)
    for cap in bp['caps']:
        cap.set_color('black')
        cap.set_linewidth(1.5)
    
    # X-axis labels - center of each transport group
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * box_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (box_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], 
                      rotation=45, ha="right")
    
    # Y-axis label
    ax.set_ylabel("Latency [ms]")
    
    # Legend for payload sizes
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'), 
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size", 
             loc='upper right', framealpha=0.95)
    
    # Clean academic styling - no grid
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)
    
    # Set y-axis to start at 0
    ax.set_ylim(bottom=0)
    
    # Tight x-axis limits
    ax.set_xlim(-0.3, group_centers[-1] + 0.5)
    
    plt.tight_layout(pad=0.5)
    
    # Save - use suffix if provided
    filename = f"latency_boxplot_by_transport{suffix}" if suffix else "latency_boxplot_by_transport"
    out_path = out_dir / f"{filename}.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / f"{filename}.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate a log-scale version if range is large
    all_max = df['max_ms'].max()
    all_min = df['min_ms'].min()
    if all_min > 0 and all_max / all_min > 50:
        plot_boxplots_grouped_by_transport_log(df, out_dir, payload_filter, suffix)


def plot_boxplots_grouped_by_transport_log(df, out_dir, payload_filter=None, suffix=""):
    """
    Generate box plots grouped by transport with log scale for large ranges.
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 16,
        'axes.labelsize': 18,
        'axes.titlesize': 20,
        'xtick.labelsize': 15,
        'ytick.labelsize': 15,
        'legend.fontsize': 14,
        'legend.title_fontsize': 15,
    })
    
    # Filter to specified payloads
    if payload_filter is not None:
        df = df[df["payload"].isin(payload_filter)]
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(), 
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    box_width = 0.25
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_transports * 1.8), 7))
    
    all_bxp_stats = []
    all_positions = []
    all_colors = []
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * box_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            if len(t_df) == 0:
                continue
            
            row = t_df.iloc[0]
            
            required = ['p25_ms', 'p50_ms', 'p75_ms']
            if not all(col in row.index for col in required):
                continue
            
            # Use P95 for upper whisker (cuts off extreme outliers)
            whislo = row.get('min_ms', row['p25_ms'])
            whishi = row.get('p95_ms', row.get('max_ms', row['p75_ms']))
            
            stats = {
                'med': row['p50_ms'],
                'q1': row['p25_ms'],
                'q3': row['p75_ms'],
                'whislo': whislo,
                'whishi': whishi,
                'fliers': [],
            }
            all_bxp_stats.append(stats)
            
            pos = base_pos + p_idx * (box_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
    
    if not all_bxp_stats:
        plt.close()
        return
    
    # Draw box plots
    bp = ax.bxp(all_bxp_stats, positions=all_positions, patch_artist=True,
                widths=box_width, showfliers=False)
    
    for patch, color in zip(bp['boxes'], all_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.85)
        patch.set_edgecolor('black')
        patch.set_linewidth(1.5)
    
    for median in bp['medians']:
        median.set_color('white')
        median.set_linewidth(2.5)
    
    for whisker in bp['whiskers']:
        whisker.set_color('black')
        whisker.set_linewidth(1.5)
    for cap in bp['caps']:
        cap.set_color('black')
        cap.set_linewidth(1.5)
    
    # X-axis labels
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * box_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (box_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], 
                      rotation=45, ha="right")
    
    # Log scale
    ax.set_yscale('log')
    ax.set_ylabel("Latency [ms] (log scale)")
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'), 
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size", 
             loc='upper right', framealpha=0.95)
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)
    
    ax.set_xlim(-0.3, group_centers[-1] + 0.5)
    
    plt.tight_layout(pad=0.5)
    
    filename = f"latency_boxplot_by_transport{suffix}_log" if suffix else "latency_boxplot_by_transport_log"
    out_path = out_dir / f"{filename}.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / f"{filename}.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_avg_with_ci_grouped_by_transport(df, out_dir, confidence=0.95):
    """
    Generate bar chart showing average latency with confidence interval error bars, grouped by transport.
    X-axis: transports, with grouped bars for each payload size.
    
    Uses n=1 to get a meaningful "prediction interval" instead of CI for the mean:
    - Bar height = average latency
    - Error bars = mean ± (z × stddev) where z depends on confidence level
    
    For 95% confidence: mean ± 1.96×stddev (covers ~95% of data if normal)
    """
    import scipy.stats as stats
    
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(), 
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    bar_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_transports * 1.8), 7))
    
    all_positions = []
    all_avg_vals = []
    all_err_lower = []  # Distance from mean to lower bound
    all_err_upper = []  # Distance from mean to upper bound
    all_colors = []
    
    # Calculate z-value for the given confidence level
    z_val = stats.norm.ppf((1 + confidence) / 2)
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            pos = base_pos + p_idx * (bar_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
            
            if len(t_df) == 0:
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            row = t_df.iloc[0]
            
            # Check required columns exist
            required = ['avg_latency_ms', 'stddev_ms']
            if not all(col in row.index for col in required):
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            mean = row['avg_latency_ms']
            stddev = row['stddev_ms']
            min_val = row.get('min_ms', 0)
            
            all_avg_vals.append(mean)
            
            # CI with n=1: mean ± z×stddev
            ci_half = z_val * stddev
            
            # Lower error bar capped so it doesn't go below min_ms
            err_lower = min(ci_half, mean - min_val) if mean > min_val else ci_half
            err_upper = ci_half
            
            all_err_lower.append(err_lower)
            all_err_upper.append(err_upper)
    
    if not all_avg_vals or all(np.isnan(v) for v in all_avg_vals):
        print("No valid data for grouped CI plot")
        plt.close()
        return
    
    avg_arr = np.array(all_avg_vals)
    err_lower = np.array(all_err_lower)
    err_upper = np.array(all_err_upper)
    positions = np.array(all_positions)
    
    # Draw bars
    bars = ax.bar(positions, avg_arr, width=bar_width, color=all_colors,
                 edgecolor='black', linewidth=0.8, alpha=0.8)
    
    # Add asymmetric error bars
    ax.errorbar(positions, avg_arr, yerr=[err_lower, err_upper], fmt='none',
               ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels - center of each transport group
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (bar_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports],
                      rotation=45, ha="right")
    
    ax.set_ylabel("Average Latency [ms]")
    
    # Legend for payload sizes
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'),
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size",
             loc='upper right', bbox_to_anchor=(1.0, 1.0))
    
    # Add subtitle explaining the error bars
    ax.set_title(f"{int(confidence*100)}% Confidence Interval", fontsize=12, style='italic')
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Set y-axis limits
    max_val = np.nanmax(avg_arr + err_upper)
    ax.set_ylim(bottom=0, top=max_val * 1.12)
    ax.set_xlim(-0.5, group_centers[-1] + 1.0)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_avg_ci_by_transport.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_avg_ci_by_transport.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate log scale version if range is large
    valid_avg = avg_arr[~np.isnan(avg_arr)]
    if len(valid_avg) > 0 and valid_avg.max() / valid_avg.min() > 20:
        plot_avg_with_ci_grouped_by_transport_log(df, out_dir, confidence)


def plot_avg_with_ci_grouped_by_transport_log(df, out_dir, confidence=0.95):
    """
    Generate bar chart showing average latency with CI error bars (log scale).
    Uses n=1 for meaningful prediction interval.
    """
    import scipy.stats as stats
    
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(),
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    bar_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_transports * 1.8), 7))
    
    all_positions = []
    all_avg_vals = []
    all_err_lower = []
    all_err_upper = []
    all_colors = []
    
    # Calculate z-value for the given confidence level
    z_val = stats.norm.ppf((1 + confidence) / 2)
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            pos = base_pos + p_idx * (bar_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
            
            if len(t_df) == 0:
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            row = t_df.iloc[0]
            
            required = ['avg_latency_ms', 'stddev_ms']
            if not all(col in row.index for col in required):
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            mean = row['avg_latency_ms']
            stddev = row['stddev_ms']
            
            all_avg_vals.append(mean)
            
            # CI with n=1: mean ± z×stddev
            ci_half = z_val * stddev
            
            # For log scale, clamp lower error to not go below 10% of mean
            err_lower = min(ci_half, mean * 0.9)
            err_upper = ci_half
            
            all_err_lower.append(err_lower)
            all_err_upper.append(err_upper)
    
    avg_arr = np.array(all_avg_vals)
    err_lower = np.array(all_err_lower)
    err_upper = np.array(all_err_upper)
    positions = np.array(all_positions)
    
    # Draw bars
    bars = ax.bar(positions, avg_arr, width=bar_width, color=all_colors,
                 edgecolor='black', linewidth=0.8, alpha=0.8)
    
    # Add asymmetric error bars for log scale
    ax.errorbar(positions, avg_arr, yerr=[err_lower, err_upper], fmt='none',
               ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (bar_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports],
                      rotation=45, ha="right")
    
    ax.set_yscale('log')
    ax.set_ylabel("Average Latency [ms] (log scale)")
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'),
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size",
             loc='upper right', bbox_to_anchor=(1.0, 1.0))
    
    ax.set_title(f"{int(confidence*100)}% Confidence Interval", fontsize=12, style='italic')
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    ax.set_xlim(-0.5, group_centers[-1] + 1.0)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_avg_ci_by_transport_log.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_avg_ci_by_transport_log.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_avg_with_stddev_grouped_by_transport(df, out_dir):
    """
    Generate bar chart showing average latency with ±1 stddev error bars, grouped by transport.
    X-axis: transports, with grouped bars for each payload size.
    
    - Bar height = average latency
    - Error bars = ±1 standard deviation (capped at min_ms for lower bound)
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(), 
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    bar_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_transports * 1.8), 7))
    
    all_positions = []
    all_avg_vals = []
    all_err_lower = []
    all_err_upper = []
    all_colors = []
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            pos = base_pos + p_idx * (bar_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
            
            if len(t_df) == 0:
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            row = t_df.iloc[0]
            
            # Check required columns exist
            required = ['avg_latency_ms', 'stddev_ms']
            if not all(col in row.index for col in required):
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            avg = row['avg_latency_ms']
            stddev = row['stddev_ms']
            min_val = row.get('min_ms', avg * 0.1)
            
            all_avg_vals.append(avg)
            
            # Error bars: ±1 stddev, but lower bound capped at min_ms
            err_lower = min(stddev, avg - min_val)  # Don't go below min
            err_upper = stddev
            
            all_err_lower.append(err_lower)
            all_err_upper.append(err_upper)
    
    if not all_avg_vals or all(np.isnan(v) for v in all_avg_vals):
        print("No valid data for avg+stddev plot")
        plt.close()
        return
    
    avg_arr = np.array(all_avg_vals)
    err_lower = np.array(all_err_lower)
    err_upper = np.array(all_err_upper)
    positions = np.array(all_positions)
    
    # Draw bars
    bars = ax.bar(positions, avg_arr, width=bar_width, color=all_colors,
                 edgecolor='black', linewidth=0.8, alpha=0.8)
    
    # Add asymmetric error bars
    ax.errorbar(positions, avg_arr, yerr=[err_lower, err_upper], fmt='none',
               ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels - center of each transport group
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (bar_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports],
                      rotation=45, ha="right")
    
    ax.set_ylabel("Average Latency [ms]")
    
    # Legend for payload sizes
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'),
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size",
             loc='upper right', bbox_to_anchor=(1.0, 1.0))
    
    ax.set_title("Error bars: ±1 Standard Deviation", fontsize=12, style='italic')
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Set y-axis limits
    max_val = np.nanmax(avg_arr + err_upper)
    ax.set_ylim(bottom=0, top=max_val * 1.12)
    ax.set_xlim(-0.5, group_centers[-1] + 1.0)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_avg_stddev_by_transport.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_avg_stddev_by_transport.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate log scale version if range is large
    valid_avg = avg_arr[~np.isnan(avg_arr)]
    if len(valid_avg) > 0 and valid_avg.max() / valid_avg.min() > 20:
        plot_avg_with_stddev_grouped_by_transport_log(df, out_dir)


def plot_avg_with_stddev_grouped_by_transport_log(df, out_dir):
    """
    Generate bar chart showing average latency with ±1 stddev error bars (log scale).
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(),
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    bar_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_transports * 1.8), 7))
    
    all_positions = []
    all_avg_vals = []
    all_err_lower = []
    all_err_upper = []
    all_colors = []
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            pos = base_pos + p_idx * (bar_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
            
            if len(t_df) == 0:
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            row = t_df.iloc[0]
            
            required = ['avg_latency_ms', 'stddev_ms']
            if not all(col in row.index for col in required):
                all_avg_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            avg = row['avg_latency_ms']
            stddev = row['stddev_ms']
            min_val = row.get('min_ms', avg * 0.1)
            
            all_avg_vals.append(avg)
            
            # For log scale, clamp lower error more aggressively
            err_lower = min(stddev, avg * 0.9)  # Don't go below 10% of avg
            err_upper = stddev
            
            all_err_lower.append(err_lower)
            all_err_upper.append(err_upper)
    
    avg_arr = np.array(all_avg_vals)
    err_lower = np.array(all_err_lower)
    err_upper = np.array(all_err_upper)
    positions = np.array(all_positions)
    
    # Draw bars
    bars = ax.bar(positions, avg_arr, width=bar_width, color=all_colors,
                 edgecolor='black', linewidth=0.8, alpha=0.8)
    
    # Add asymmetric error bars
    ax.errorbar(positions, avg_arr, yerr=[err_lower, err_upper], fmt='none',
               ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (bar_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports],
                      rotation=45, ha="right")
    
    ax.set_yscale('log')
    ax.set_ylabel("Average Latency [ms] (log scale)")
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'),
                  edgecolor='black', label=label, alpha=0.8)
        )
    ax.legend(handles=legend_elements, title="Payload Size",
             loc='upper right', bbox_to_anchor=(1.0, 1.0))
    
    ax.set_title("Error bars: ±1 Standard Deviation", fontsize=12, style='italic')
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    ax.set_xlim(-0.5, group_centers[-1] + 1.0)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_avg_stddev_by_transport_log.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_avg_stddev_by_transport_log.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_median_with_error_grouped_by_transport(df, out_dir):
    """
    Generate bar chart showing median (P50) latency with min/P95 error bars, grouped by transport.
    X-axis: transports, with grouped bars for each payload size.
    
    - Bar height = median (P50)
    - Lower error bar = down to min
    - Upper error bar = up to P95
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 20,
        'axes.labelsize': 20,
        'axes.titlesize': 20,
        'xtick.labelsize': 20,
        'ytick.labelsize': 20,
        'legend.fontsize': 16,
    })
    
    payloads = sorted(df["payload"].unique())
    transports = sorted(df["transport"].unique(), 
                       key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    n_payloads = len(payloads)
    n_transports = len(transports)
    
    bar_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(18, 7)) # Wide format
    
    all_positions = []
    all_median_vals = []
    all_err_lower = []
    all_err_upper = []
    all_colors = []
    
    for t_idx, transport in enumerate(transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        
        for p_idx, payload in enumerate(payloads):
            t_df = df[(df["transport"] == transport) & (df["payload"] == payload)]
            
            pos = base_pos + p_idx * (bar_width + 0.02)
            all_positions.append(pos)
            all_colors.append(PAYLOAD_COLORS.get(payload, '#1f77b4'))
            
            if len(t_df) == 0:
                all_median_vals.append(np.nan)
                all_err_lower.append(0)
                all_err_upper.append(0)
                continue
            
            row = t_df.iloc[0]
            
            # Get P50, min, and P95
            median = row.get('p50_ms', np.nan)
            min_val = row.get('min_ms', median * 0.5)
            p95 = row.get('p95_ms', row.get('max_ms', median * 2))
            
            all_median_vals.append(median)
            
            # For log scale, clamp lower error to 90% of median
            err_lower = min(median - min_val, median * 0.9) if median > min_val else 0
            err_upper = p95 - median
            
            all_err_lower.append(err_lower)
            all_err_upper.append(err_upper)
    
    if not all_median_vals or all(np.isnan(v) for v in all_median_vals):
        print("No valid data for median+error plot")
        plt.close()
        return
    
    median_arr = np.array(all_median_vals)
    err_lower = np.array(all_err_lower)
    err_upper = np.array(all_err_upper)
    positions = np.array(all_positions)
    
    # Draw bars
    bars = ax.bar(positions, median_arr, width=bar_width, color=all_colors,
                 edgecolor='black', linewidth=0.8, alpha=0.85)
    
    # Add asymmetric error bars (min to P95)
    ax.errorbar(positions, median_arr, yerr=[err_lower, err_upper], fmt='none',
               ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels - center of each transport group
    group_centers = []
    for t_idx in range(n_transports):
        base_pos = t_idx * (n_payloads * bar_width + group_spacing + 0.3)
        center = base_pos + (n_payloads - 1) * (bar_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    
    # Custom labels with RabbitMQ split into two lines
    def format_transport_label(t):
        name = TRANSPORT_NAMES.get(t, t)
        if name == "AMQP-RabbitMQ":
            return "RabbitMQ\n(AMQP)"
        elif name == "MQTT-RabbitMQ":
            return "RabbitMQ\n(MQTT)"
        return name
    
    ax.set_xticklabels([format_transport_label(t) for t in transports],
                      rotation=0, ha="center")
    
    ax.set_yscale('log')
    ax.set_ylabel("Latency [ms]")
    
    # Legend for payload sizes - above horizontally
    from matplotlib.patches import Patch
    legend_elements = []
    for payload in payloads:
        if payload >= 1048576:
            label = f"{payload // 1048576} MB"
        elif payload >= 1024:
            label = f"{payload // 1024} KB"
        else:
            label = f"{payload} B"
        legend_elements.append(
            Patch(facecolor=PAYLOAD_COLORS.get(payload, '#1f77b4'),
                  edgecolor='black', label=label, alpha=0.85)
        )
    ax.legend(handles=legend_elements, loc='upper center', 
              bbox_to_anchor=(0.5, 1.12), ncol=n_payloads, framealpha=0.9)
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    ax.set_xlim(-0.5, group_centers[-1] + 1.0)
    
    plt.tight_layout()
    
    out_path = out_dir / "latency_median_error_by_transport.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "latency_median_error_by_transport.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_avg_with_ci(df, out_dir, confidence=0.95):
    """
    Generate bar charts showing average latency with confidence interval error bars.
    
    CI = mean ± (z * stddev / sqrt(n))
    where z ≈ 1.96 for 95% CI
    """
    import scipy.stats as stats
    
    # Use serif font for academic style
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Get unique payloads in the data
    payloads = sorted(df["payload"].unique())
    
    for payload in payloads:
        payload_df = df[df["payload"] == payload]
        
        if payload_df.empty:
            continue
        
        # Get transports sorted by display name
        transports = sorted(payload_df["transport"].unique(), 
                          key=lambda t: TRANSPORT_NAMES.get(t, t))
        
        fig, ax = plt.subplots(figsize=(max(8, len(transports) * 1.2), 6))
        
        avg_vals = []
        ci_vals = []
        colors = []
        
        for transport in transports:
            t_df = payload_df[payload_df["transport"] == transport]
            if len(t_df) == 0:
                avg_vals.append(np.nan)
                ci_vals.append(0)
                colors.append('#1f77b4')
                continue
            
            row = t_df.iloc[0]
            
            # Check required columns exist
            required = ['avg_latency_ms', 'stddev_ms', 'sample_count']
            if not all(col in row.index for col in required):
                print(f"Warning: Missing columns for CI plot. Required: {required}")
                avg_vals.append(np.nan)
                ci_vals.append(0)
                colors.append('#1f77b4')
                continue
            
            mean = row['avg_latency_ms']
            stddev = row['stddev_ms']
            n = row['sample_count']
            
            avg_vals.append(mean)
            
            # Calculate confidence interval half-width
            if n > 1 and stddev > 0:
                # Use t-distribution for smaller samples, normal for large
                if n < 30:
                    t_val = stats.t.ppf((1 + confidence) / 2, n - 1)
                    ci_half = t_val * stddev / np.sqrt(n)
                else:
                    z_val = stats.norm.ppf((1 + confidence) / 2)
                    ci_half = z_val * stddev / np.sqrt(n)
            else:
                ci_half = 0
            
            ci_vals.append(ci_half)
            colors.append(TRANSPORT_COLORS.get(transport, '#1f77b4'))
        
        avg_arr = np.array(avg_vals)
        ci_arr = np.array(ci_vals)
        
        x = np.arange(len(transports))
        
        # Draw bars with error bars
        bars = ax.bar(x, avg_arr, width=0.6, color=colors, 
                     edgecolor='black', linewidth=0.8, alpha=0.8)
        
        # Add error bars for confidence interval
        ax.errorbar(x, avg_arr, yerr=ci_arr, fmt='none', 
                   ecolor='black', capsize=5, capthick=2, elinewidth=2)
        
        # Add value labels above error bars
        for i, (avg, ci) in enumerate(zip(avg_vals, ci_vals)):
            if not np.isnan(avg):
                # Format based on magnitude
                if avg >= 100:
                    label = f"{avg:.1f}"
                elif avg >= 10:
                    label = f"{avg:.2f}"
                else:
                    label = f"{avg:.3f}"
                
                # Position label above error bar
                y_pos = avg + ci + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02
                ax.annotate(label,
                           xy=(i, y_pos),
                           ha="center", va="bottom", fontsize=11, fontweight='medium')
        
        # X-axis labels
        ax.set_xticks(x)
        ax.set_xticklabels([TRANSPORT_NAMES.get(t, t) for t in transports], 
                          rotation=45, ha="right")
        
        # Y-axis label
        ax.set_ylabel("Average Latency [ms]")
        
        # Add subtitle for CI info
        ax.set_title(f"{int(confidence*100)}% Confidence Interval", fontsize=12, style='italic')
        
        # Clean academic styling
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
        ax.set_axisbelow(True)
        
        # Set y-axis to start at 0 with headroom for labels
        max_val = np.nanmax(avg_arr + ci_arr)
        ax.set_ylim(bottom=0, top=max_val * 1.15)
        
        plt.tight_layout()
        
        # Save
        if payload >= 1048576:
            payload_suffix = f"{payload // 1048576}MB"
        elif payload >= 1024:
            payload_suffix = f"{payload // 1024}KB"
        else:
            payload_suffix = f"{payload}B"
        
        out_path = out_dir / f"latency_avg_ci_{payload_suffix}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
        
        out_path_pdf = out_dir / f"latency_avg_ci_{payload_suffix}.pdf"
        plt.savefig(out_path_pdf, bbox_inches='tight')
        print(f"Saved: {out_path_pdf}")
        
        plt.close()


# =============================================================================
# QoS Data Format Plotting Functions
# These functions work with CSV files that have 'broker' and 'qos' columns
# directly (e.g., good/qos/summary.csv format)
# =============================================================================

def plot_qos_avg_with_ci(df, out_dir, confidence=0.95):
    """
    Generate bar chart showing average latency with confidence interval error bars.
    Groups bars by broker with QoS 0/1/2 as grouped bars.
    
    Uses lat_mean_ms and lat_stdev_ms columns from QoS summary format.
    Error bars: mean ± (z × stddev) for the given confidence level.
    """
    import scipy.stats as stats
    
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    # Check required columns
    if 'broker' not in df.columns or 'qos' not in df.columns:
        print("Error: QoS data format requires 'broker' and 'qos' columns")
        return
    
    if 'lat_mean_ms' not in df.columns or 'lat_stdev_ms' not in df.columns:
        print("Error: QoS data format requires 'lat_mean_ms' and 'lat_stdev_ms' columns")
        return
    
    # Get unique brokers and QoS levels
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    
    bar_width = 0.25
    
    fig, ax = plt.subplots(figsize=(max(10, n_brokers * 2), 6))
    
    # Calculate z-value for the given confidence level
    z_val = stats.norm.ppf((1 + confidence) / 2)
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        means = []
        errors_lower = []
        errors_upper = []
        
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                row = broker_df.iloc[0]
                mean = row['lat_mean_ms']
                stddev = row['lat_stdev_ms']
                min_val = row.get('lat_min_ms', 0)
                
                means.append(mean)
                
                # CI: mean ± z×stddev
                ci_half = z_val * stddev
                # Lower error capped at min_ms
                err_lower = min(ci_half, mean - min_val) if mean > min_val else ci_half
                err_upper = ci_half
                
                errors_lower.append(err_lower)
                errors_upper.append(err_upper)
            else:
                means.append(np.nan)
                errors_lower.append(0)
                errors_upper.append(0)
        
        x = np.arange(n_brokers)
        offset = (i - (n_qos - 1) / 2) * bar_width
        
        bars = ax.bar(x + offset, means, bar_width,
                      label=QOS_LABELS.get(qos, f'QoS {qos}'),
                      color=QOS_COLORS.get(qos, '#1f77b4'),
                      edgecolor='black', linewidth=0.8, alpha=0.85)
        
        # Add error bars
        ax.errorbar(x + offset, means, yerr=[errors_lower, errors_upper],
                   fmt='none', ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    # X-axis labels
    ax.set_xticks(np.arange(n_brokers))
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    ax.set_ylabel("Average Latency [ms]")
    ax.set_xlabel("MQTT Broker")
    ax.set_title(f"Average Latency with {int(confidence*100)}% Confidence Interval", fontsize=14)
    
    ax.legend(title="QoS Level", loc='upper right', bbox_to_anchor=(1.18, 1.0))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_avg_ci.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_avg_ci.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate log scale if range is large
    valid_means = [m for m in df['lat_mean_ms'] if not np.isnan(m)]
    if len(valid_means) > 1 and max(valid_means) / min(valid_means) > 20:
        plot_qos_avg_with_ci_log(df, out_dir, confidence)


def plot_qos_avg_with_ci_log(df, out_dir, confidence=0.95):
    """Log-scale version of QoS average latency with CI."""
    import scipy.stats as stats
    
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    bar_width = 0.25
    
    fig, ax = plt.subplots(figsize=(max(10, n_brokers * 2), 6))
    
    z_val = stats.norm.ppf((1 + confidence) / 2)
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        means = []
        errors_lower = []
        errors_upper = []
        
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                row = broker_df.iloc[0]
                mean = row['lat_mean_ms']
                stddev = row['lat_stdev_ms']
                
                means.append(mean)
                ci_half = z_val * stddev
                # For log scale, clamp lower error to 90% of mean
                err_lower = min(ci_half, mean * 0.9)
                err_upper = ci_half
                
                errors_lower.append(err_lower)
                errors_upper.append(err_upper)
            else:
                means.append(np.nan)
                errors_lower.append(0)
                errors_upper.append(0)
        
        x = np.arange(n_brokers)
        offset = (i - (n_qos - 1) / 2) * bar_width
        
        bars = ax.bar(x + offset, means, bar_width,
                      label=QOS_LABELS.get(qos, f'QoS {qos}'),
                      color=QOS_COLORS.get(qos, '#1f77b4'),
                      edgecolor='black', linewidth=0.8, alpha=0.85)
        
        ax.errorbar(x + offset, means, yerr=[errors_lower, errors_upper],
                   fmt='none', ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    ax.set_xticks(np.arange(n_brokers))
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    ax.set_yscale('log')
    ax.set_ylabel("Average Latency [ms] (log scale)")
    ax.set_xlabel("MQTT Broker")
    ax.set_title(f"Average Latency with {int(confidence*100)}% CI (Log Scale)", fontsize=14)
    
    ax.legend(title="QoS Level", loc='upper right', bbox_to_anchor=(1.18, 1.0))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_avg_ci_log.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_avg_ci_log.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_qos_boxplots(df, out_dir):
    """
    Generate box plots for latency distribution by broker and QoS.
    Uses summary statistics from QoS data format to construct box plots.
    
    Box: Q1 (25th) to Q3 (75th percentile) - approximated from available data
    Whiskers: min to P95
    Line: median (P50)
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    if 'broker' not in df.columns or 'qos' not in df.columns:
        print("Error: QoS data format requires 'broker' and 'qos' columns")
        return
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    
    box_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_brokers * 2.5), 7))
    
    all_bxp_stats = []
    all_positions = []
    all_colors = []
    
    for b_idx, broker in enumerate(brokers):
        base_pos = b_idx * (n_qos * box_width + group_spacing + 0.3)
        
        for q_idx, qos in enumerate(qos_levels):
            qos_df = df[(df["broker"] == broker) & (df["qos"] == qos)]
            
            if len(qos_df) == 0:
                continue
            
            row = qos_df.iloc[0]
            
            # Get available percentile data
            p50 = row.get('lat_p50_ms', row.get('lat_mean_ms', np.nan))
            min_val = row.get('lat_min_ms', p50 * 0.5)
            p95 = row.get('lat_p95_ms', row.get('lat_max_ms', p50 * 2))
            
            # Estimate Q1 and Q3 from available data
            # If we have mean and stddev, estimate quartiles assuming roughly normal
            mean = row.get('lat_mean_ms', p50)
            stddev = row.get('lat_stdev_ms', (p95 - min_val) / 4)
            
            # Estimate Q1 ~ mean - 0.67*stddev, Q3 ~ mean + 0.67*stddev
            q1 = max(min_val, mean - 0.67 * stddev)
            q3 = min(p95, mean + 0.67 * stddev)
            
            # Ensure ordering: min <= q1 <= p50 <= q3 <= p95
            q1 = min(q1, p50)
            q3 = max(q3, p50)
            
            stats_dict = {
                'med': p50,
                'q1': q1,
                'q3': q3,
                'whislo': min_val,
                'whishi': p95,
                'fliers': [],
            }
            all_bxp_stats.append(stats_dict)
            
            pos = base_pos + q_idx * (box_width + 0.02)
            all_positions.append(pos)
            all_colors.append(QOS_COLORS.get(qos, '#1f77b4'))
    
    if not all_bxp_stats:
        print("No valid data for QoS box plots")
        plt.close()
        return
    
    # Draw box plots
    bp = ax.bxp(all_bxp_stats, positions=all_positions, patch_artist=True,
                widths=box_width, showfliers=False)
    
    # Color the boxes by QoS
    for patch, color in zip(bp['boxes'], all_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.8)
        patch.set_edgecolor('black')
        patch.set_linewidth(1.2)
    
    # Style median lines
    for median in bp['medians']:
        median.set_color('black')
        median.set_linewidth(2)
    
    # Style whiskers and caps
    for whisker in bp['whiskers']:
        whisker.set_color('black')
        whisker.set_linewidth(1.2)
    for cap in bp['caps']:
        cap.set_color('black')
        cap.set_linewidth(1.2)
    
    # X-axis labels - center of each broker group
    group_centers = []
    for b_idx in range(n_brokers):
        base_pos = b_idx * (n_qos * box_width + group_spacing + 0.3)
        center = base_pos + (n_qos - 1) * (box_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    ax.set_ylabel("Latency [ms]")
    ax.set_xlabel("MQTT Broker")
    ax.set_title("Latency Distribution by QoS Level", fontsize=14)
    
    # Legend for QoS levels
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=QOS_COLORS.get(qos, '#1f77b4'), edgecolor='black',
              label=QOS_LABELS.get(qos, f'QoS {qos}'), alpha=0.8)
        for qos in qos_levels
    ]
    ax.legend(handles=legend_elements, title="QoS Level",
             loc='upper right', bbox_to_anchor=(1.18, 1.0))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_boxplot.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_boxplot.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate log scale if range is large
    all_p95 = df['lat_p95_ms'].dropna() if 'lat_p95_ms' in df.columns else df['lat_max_ms'].dropna()
    all_min = df['lat_min_ms'].dropna() if 'lat_min_ms' in df.columns else df['lat_p50_ms'].dropna()
    if len(all_p95) > 0 and len(all_min) > 0 and all_min.min() > 0:
        if all_p95.max() / all_min.min() > 20:
            plot_qos_boxplots_log(df, out_dir)


def plot_qos_boxplots_log(df, out_dir):
    """Log-scale version of QoS box plots."""
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    
    box_width = 0.22
    group_spacing = 0.15
    
    fig, ax = plt.subplots(figsize=(max(12, n_brokers * 2.5), 7))
    
    all_bxp_stats = []
    all_positions = []
    all_colors = []
    
    for b_idx, broker in enumerate(brokers):
        base_pos = b_idx * (n_qos * box_width + group_spacing + 0.3)
        
        for q_idx, qos in enumerate(qos_levels):
            qos_df = df[(df["broker"] == broker) & (df["qos"] == qos)]
            
            if len(qos_df) == 0:
                continue
            
            row = qos_df.iloc[0]
            
            p50 = row.get('lat_p50_ms', row.get('lat_mean_ms', np.nan))
            min_val = row.get('lat_min_ms', p50 * 0.5)
            p95 = row.get('lat_p95_ms', row.get('lat_max_ms', p50 * 2))
            
            mean = row.get('lat_mean_ms', p50)
            stddev = row.get('lat_stdev_ms', (p95 - min_val) / 4)
            
            q1 = max(min_val, mean - 0.67 * stddev)
            q3 = min(p95, mean + 0.67 * stddev)
            q1 = min(q1, p50)
            q3 = max(q3, p50)
            
            # Ensure positive values for log scale
            min_val = max(min_val, 0.001)
            q1 = max(q1, 0.001)
            
            stats_dict = {
                'med': p50,
                'q1': q1,
                'q3': q3,
                'whislo': min_val,
                'whishi': p95,
                'fliers': [],
            }
            all_bxp_stats.append(stats_dict)
            
            pos = base_pos + q_idx * (box_width + 0.02)
            all_positions.append(pos)
            all_colors.append(QOS_COLORS.get(qos, '#1f77b4'))
    
    if not all_bxp_stats:
        plt.close()
        return
    
    bp = ax.bxp(all_bxp_stats, positions=all_positions, patch_artist=True,
                widths=box_width, showfliers=False)
    
    for patch, color in zip(bp['boxes'], all_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.8)
        patch.set_edgecolor('black')
        patch.set_linewidth(1.2)
    
    for median in bp['medians']:
        median.set_color('black')
        median.set_linewidth(2)
    
    for whisker in bp['whiskers']:
        whisker.set_color('black')
        whisker.set_linewidth(1.2)
    for cap in bp['caps']:
        cap.set_color('black')
        cap.set_linewidth(1.2)
    
    group_centers = []
    for b_idx in range(n_brokers):
        base_pos = b_idx * (n_qos * box_width + group_spacing + 0.3)
        center = base_pos + (n_qos - 1) * (box_width + 0.02) / 2
        group_centers.append(center)
    
    ax.set_xticks(group_centers)
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    ax.set_yscale('log')
    ax.set_ylabel("Latency [ms] (log scale)")
    ax.set_xlabel("MQTT Broker")
    ax.set_title("Latency Distribution by QoS Level (Log Scale)", fontsize=14)
    
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=QOS_COLORS.get(qos, '#1f77b4'), edgecolor='black',
              label=QOS_LABELS.get(qos, f'QoS {qos}'), alpha=0.8)
        for qos in qos_levels
    ]
    ax.legend(handles=legend_elements, title="QoS Level",
             loc='upper right', bbox_to_anchor=(1.18, 1.0))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_boxplot_log.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_boxplot_log.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_qos_median_with_error(df, out_dir):
    """
    Generate bar chart with median (P50) and asymmetric error bars from min to P95.
    Groups bars by broker with QoS 0/1/2 as grouped bars.
    
    Bar height: median (P50)
    Lower error: down to min
    Upper error: up to P95
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 26,
        'axes.labelsize': 26,
        'axes.titlesize': 26,
        'xtick.labelsize': 26,
        'ytick.labelsize': 26,
        'legend.fontsize': 26,
    })
    
    if 'broker' not in df.columns or 'qos' not in df.columns:
        print("Error: QoS data format requires 'broker' and 'qos' columns")
        return
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    
    bar_width = 0.25
    
    fig, ax = plt.subplots(figsize=(max(14, n_brokers * 2.5), 8))
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        medians = []
        errors_lower = []  # Distance from median to min
        errors_upper = []  # Distance from median to P95
        
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                row = broker_df.iloc[0]
                median = row.get('lat_p50_ms', row.get('lat_mean_ms', np.nan))
                min_val = row.get('lat_min_ms', median * 0.5)
                p95 = row.get('lat_p95_ms', row.get('lat_max_ms', median * 2))
                
                medians.append(median)
                # For log scale, clamp lower error to 90% of median
                errors_lower.append(min(median - min_val, median * 0.9))
                errors_upper.append(p95 - median)
            else:
                medians.append(np.nan)
                errors_lower.append(0)
                errors_upper.append(0)
        
        x = np.arange(n_brokers)
        offset = (i - (n_qos - 1) / 2) * bar_width
        
        bars = ax.bar(x + offset, medians, bar_width,
                      label=f'QoS {qos}',
                      color=QOS_COLORS.get(qos, '#1f77b4'),
                      edgecolor='black', linewidth=0.8, alpha=0.85)
        
        # Add asymmetric error bars (min to P95)
        ax.errorbar(x + offset, medians, yerr=[errors_lower, errors_upper],
                   fmt='none', ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    ax.set_xticks(np.arange(n_brokers))
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=0, ha="center")
    
    ax.set_yscale('log')
    ax.set_ylabel("Latency [ms]")
    
    # Legend above horizontally, no title, smaller font
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=n_qos, framealpha=0.9, fontsize=18)
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_median_error.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_median_error.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()
    
    # Also generate log scale if range is large
    all_p95 = df['lat_p95_ms'].dropna() if 'lat_p95_ms' in df.columns else df['lat_max_ms'].dropna()
    all_min = df['lat_min_ms'].dropna() if 'lat_min_ms' in df.columns else df['lat_p50_ms'].dropna()
    if len(all_p95) > 0 and len(all_min) > 0 and all_min.min() > 0:
        if all_p95.max() / all_min.min() > 20:
            plot_qos_median_with_error_log(df, out_dir)


def plot_qos_median_with_error_log(df, out_dir):
    """Log-scale version of QoS median with error bars."""
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 18,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
    })
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_brokers = len(brokers)
    n_qos = len(qos_levels)
    bar_width = 0.25
    
    fig, ax = plt.subplots(figsize=(max(10, n_brokers * 2), 6))
    
    for i, qos in enumerate(qos_levels):
        qos_df = df[df["qos"] == qos]
        
        medians = []
        errors_lower = []
        errors_upper = []
        
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                row = broker_df.iloc[0]
                median = row.get('lat_p50_ms', row.get('lat_mean_ms', np.nan))
                min_val = row.get('lat_min_ms', median * 0.5)
                p95 = row.get('lat_p95_ms', row.get('lat_max_ms', median * 2))
                
                medians.append(median)
                # For log scale, clamp lower error to 90% of median
                errors_lower.append(min(median - min_val, median * 0.9))
                errors_upper.append(p95 - median)
            else:
                medians.append(np.nan)
                errors_lower.append(0)
                errors_upper.append(0)
        
        x = np.arange(n_brokers)
        offset = (i - (n_qos - 1) / 2) * bar_width
        
        bars = ax.bar(x + offset, medians, bar_width,
                      label=QOS_LABELS.get(qos, f'QoS {qos}'),
                      color=QOS_COLORS.get(qos, '#1f77b4'),
                      edgecolor='black', linewidth=0.8, alpha=0.85)
        
        ax.errorbar(x + offset, medians, yerr=[errors_lower, errors_upper],
                   fmt='none', ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
    
    ax.set_xticks(np.arange(n_brokers))
    ax.set_xticklabels([MQTT_BROKER_NAMES.get(b, b) for b in brokers], rotation=30, ha="right")
    
    ax.set_yscale('log')
    ax.set_ylabel("Latency [ms] (log scale)")
    ax.set_xlabel("MQTT Broker")
    ax.set_title("Median Latency (P50) with Min/P95 Error Bars (Log Scale)", fontsize=14)
    
    ax.legend(title="QoS Level", loc='upper right', bbox_to_anchor=(1.18, 1.0))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_latency_median_error_log.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_latency_median_error_log.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def plot_qos_comparison_grouped(df, out_dir):
    """
    Generate grouped comparison chart showing all brokers for each QoS level.
    Three side-by-side grouped bar charts (one per QoS) for easy visual comparison.
    """
    plt.rcParams.update({
        'font.family': 'serif',
        'font.size': 12,
        'axes.labelsize': 14,
        'axes.titlesize': 14,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
    })
    
    if 'broker' not in df.columns or 'qos' not in df.columns:
        print("Error: QoS data format requires 'broker' and 'qos' columns")
        return
    
    brokers = sorted(df["broker"].unique(), key=lambda b: MQTT_BROKER_NAMES.get(b, b))
    qos_levels = sorted(df["qos"].unique())
    
    n_qos = len(qos_levels)
    
    fig, axes = plt.subplots(1, n_qos, figsize=(5 * n_qos, 5), sharey=True)
    if n_qos == 1:
        axes = [axes]
    
    for ax, qos in zip(axes, qos_levels):
        qos_df = df[df["qos"] == qos]
        
        medians = []
        errors_lower = []
        errors_upper = []
        broker_names = []
        
        for broker in brokers:
            broker_df = qos_df[qos_df["broker"] == broker]
            if len(broker_df) > 0:
                row = broker_df.iloc[0]
                median = row.get('lat_p50_ms', row.get('lat_mean_ms', np.nan))
                min_val = row.get('lat_min_ms', median * 0.5)
                p95 = row.get('lat_p95_ms', row.get('lat_max_ms', median * 2))
                
                medians.append(median)
                errors_lower.append(median - min_val)
                errors_upper.append(p95 - median)
                broker_names.append(MQTT_BROKER_NAMES.get(broker, broker))
            else:
                medians.append(np.nan)
                errors_lower.append(0)
                errors_upper.append(0)
                broker_names.append(MQTT_BROKER_NAMES.get(broker, broker))
        
        x = np.arange(len(brokers))
        
        bars = ax.bar(x, medians, width=0.6,
                      color=QOS_COLORS.get(qos, '#1f77b4'),
                      edgecolor='black', linewidth=0.8, alpha=0.85)
        
        ax.errorbar(x, medians, yerr=[errors_lower, errors_upper],
                   fmt='none', ecolor='black', capsize=4, capthick=1.5, elinewidth=1.5)
        
        ax.set_xticks(x)
        ax.set_xticklabels(broker_names, rotation=45, ha="right")
        ax.set_title(QOS_LABELS.get(qos, f'QoS {qos}'), fontsize=14, fontweight='bold')
        
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.yaxis.grid(True, linestyle='--', alpha=0.4, linewidth=0.5)
        ax.set_axisbelow(True)
        ax.set_ylim(bottom=0)
    
    axes[0].set_ylabel("Latency [ms]")
    
    fig.suptitle("Latency Comparison by QoS Level", fontsize=16, fontweight='bold', y=1.02)
    
    plt.tight_layout()
    
    out_path = out_dir / "qos_comparison_grouped.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {out_path}")
    
    out_path_pdf = out_dir / "qos_comparison_grouped.pdf"
    plt.savefig(out_path_pdf, bbox_inches='tight')
    print(f"Saved: {out_path_pdf}")
    
    plt.close()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.summary)
    
    # Handle QoS data format first (before filtering by sub_tps which may not exist)
    if args.qos_data or args.all_qos_plots:
        # QoS data format mode - CSV has broker,qos columns directly
        # Generate all QoS-specific plots
        print("Generating QoS data plots...")
        
        # 1. Average latency with 95% CI error bars for each broker and QoS
        plot_qos_avg_with_ci(df, out_dir, confidence=args.confidence)
        
        # 2. Box plots for each broker and QoS  
        plot_qos_boxplots(df, out_dir)
        
        # 3. Bar charts with median and min/p95 error bars
        plot_qos_median_with_error(df, out_dir)
        
        # 4. Grouped comparison across all brokers and QoS
        plot_qos_comparison_grouped(df, out_dir)
        
        return
    
    # Filter out failed runs (sub_tps < 10% of rate) - only for non-QoS data
    if "sub_tps" in df.columns and "rate" in df.columns:
        df = df[df["sub_tps"] >= df["rate"] * 0.1]
    
    if args.qos:
        # QoS comparison mode
        plot_qos_comparison(df, out_dir, payload=args.payload)
        plot_loss_comparison(df, out_dir, payload=args.payload)
        
        return
    
    if args.boxplot:
        # Box plot mode - generate separate charts for small and large payloads
        # 1KB + 16KB together (similar scale)
        plot_boxplots_grouped_by_transport(df, out_dir, 
                                           payload_filter=[1024, 16384], 
                                           suffix="_1KB_16KB")
        # 1MB separately (different scale)
        plot_boxplots_grouped_by_transport(df, out_dir, 
                                           payload_filter=[1048576], 
                                           suffix="_1MB")
        # All payloads combined
        plot_boxplots_grouped_by_transport(df, out_dir)
        # Also generate per-payload versions
        plot_boxplots(df, out_dir)
        return
    
    if args.ci:
        # Median + IQR mode - grouped by transport
        plot_avg_with_ci_grouped_by_transport(df, out_dir, confidence=args.confidence)
        # Also generate per-payload versions
        plot_avg_with_ci(df, out_dir, confidence=args.confidence)
        return
    
    if args.avg:
        # Average + Stddev mode - grouped by transport
        plot_avg_with_stddev_grouped_by_transport(df, out_dir)
        return
    
    if args.median_error:
        # Median (P50) with min/P95 error bars - grouped by transport
        plot_median_with_error_grouped_by_transport(df, out_dir)
        return
    
    # Standard payload comparison mode
    # Filter to selected payloads
    df = df[df["payload"].isin(PAYLOADS)]
    
    # Get unique transports in order
    transports = df["transport"].unique()
    # Sort by display name
    transports = sorted(transports, key=lambda t: TRANSPORT_NAMES.get(t, t))
    
    # Generate separate charts per payload (P50 only)
    plot_per_payload(df, transports, out_dir)
    
    # Generate separate charts per payload (P50 + P99 combined)
    plot_per_payload_p50_p99(df, transports, out_dir)
    
    # Generate combined chart with all payloads
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
                      color=PAYLOAD_COLORS[payload],
                      edgecolor="black", linewidth=0.5)
        
        # Add error bars (P99 whiskers)
        ax.errorbar(x + offset, p50_arr, yerr=[yerr_lower, yerr_upper],
                    fmt='none', ecolor='black', capsize=3, capthick=1.5, elinewidth=1.5)
    
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
        from matplotlib.ticker import ScalarFormatter, FixedLocator
        ax.set_yscale("log")
        ax.set_ylabel("Latency [ms] (log scale)", fontsize=12)
        # Use scalar formatting instead of 10^x notation
        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.get_major_formatter().set_scientific(False)
        # Set clean tick locations: 0.5, 1, 2, 3 | 5, 10, 15, 20...
        tick_vals = [0.2, 0.5, 1, 2, 3, 5, 10, 15, 20, 50, 100, 200, 500]
        tick_vals = [t for t in tick_vals if min_val * 0.8 <= t <= max_val * 1.2]
        ax.yaxis.set_major_locator(FixedLocator(tick_vals))
    
    ax.legend(title="Payload Size", loc="upper right")
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
