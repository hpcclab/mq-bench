#!/usr/bin/env python3
"""
Plot QoS comparison results as horizontal bar charts.

Generates:
  - Horizontal bar chart: P50/P95/P99 latency by broker × QoS level
  - Only includes brokers with valid QoS data (skips if missing)
  - Color-coded by QoS level (0=green, 1=yellow, 2=red)

Usage:
  python3 plot_qos_comparison.py --summary results/qos_comparison_*/raw_data/summary.csv --out-dir results/qos_comparison_*/plots
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

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


def load_summary(csv_path: str) -> pd.DataFrame:
    """Load and validate summary CSV."""
    df = pd.read_csv(csv_path)
    
    # Ensure required columns exist
    required = ['broker', 'qos', 'p50_ms', 'p95_ms', 'p99_ms']
    missing = [c for c in required if c not in df.columns]
    if missing:
        # Try alternative column names
        if 'transport' in df.columns and 'broker' not in df.columns:
            # Extract broker from transport (e.g., mqtt_mosquitto -> mosquitto)
            df['broker'] = df['transport'].str.replace('mqtt_', '', regex=False)
        else:
            raise ValueError(f"Missing required columns: {missing}")
    
    # Filter to valid QoS rows
    df = df[df['qos'].notna() & df['qos'].isin([0, 1, 2])].copy()
    df['qos'] = df['qos'].astype(int)
    
    # Convert latency columns to numeric
    for col in ['p50_ms', 'p95_ms', 'p99_ms']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def plot_latency_bars(df: pd.DataFrame, latency_col: str, title: str, out_path: Path):
    """
    Create horizontal bar chart for latency by broker × QoS.
    
    Y-axis: Broker names (grouped)
    X-axis: Latency in ms
    Bars: Color-coded by QoS level
    """
    # Get unique brokers and QoS levels present in data
    brokers = sorted(df['broker'].unique())
    qos_levels = sorted(df['qos'].unique())
    
    if len(brokers) == 0:
        print(f"WARN: No brokers found for {latency_col}")
        return
    
    # Prepare data: for each broker, get latency for each QoS level (if available)
    bar_data = []
    for broker in brokers:
        broker_df = df[df['broker'] == broker]
        for qos in qos_levels:
            qos_df = broker_df[broker_df['qos'] == qos]
            if not qos_df.empty and pd.notna(qos_df[latency_col].iloc[0]):
                latency = qos_df[latency_col].iloc[0]
                bar_data.append({
                    'broker': broker,
                    'qos': qos,
                    'latency': latency,
                })
    
    if not bar_data:
        print(f"WARN: No valid latency data for {latency_col}")
        return
    
    bar_df = pd.DataFrame(bar_data)
    
    # Calculate positions for grouped bars
    n_qos = len(qos_levels)
    bar_height = 0.8 / n_qos
    
    fig, ax = plt.subplots(figsize=(12, max(6, len(brokers) * 0.8)))
    
    # Plot bars for each QoS level
    for i, qos in enumerate(qos_levels):
        qos_data = bar_df[bar_df['qos'] == qos]
        
        # Get positions for this QoS level's bars
        y_positions = []
        latencies = []
        for j, broker in enumerate(brokers):
            broker_qos_data = qos_data[qos_data['broker'] == broker]
            if not broker_qos_data.empty:
                y_positions.append(j + (i - n_qos/2 + 0.5) * bar_height)
                latencies.append(broker_qos_data['latency'].iloc[0])
        
        if y_positions:
            ax.barh(
                y_positions,
                latencies,
                height=bar_height * 0.9,
                color=QOS_COLORS.get(qos, '#999999'),
                label=QOS_LABELS.get(qos, f'QoS {qos}'),
                edgecolor='white',
                linewidth=0.5,
            )
            
            # Add value labels on bars
            for y, lat in zip(y_positions, latencies):
                ax.text(
                    lat + ax.get_xlim()[1] * 0.01,
                    y,
                    f'{lat:.2f}',
                    va='center',
                    fontsize=9,
                )
    
    # Customize axes
    ax.set_yticks(range(len(brokers)))
    ax.set_yticklabels(brokers)
    ax.set_xlabel('Latency (ms)')
    ax.set_title(title)
    
    # Add legend
    ax.legend(loc='lower right')
    
    # Add gridlines
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_combined_latencies(df: pd.DataFrame, out_dir: Path):
    """
    Create a combined figure with P50, P95, P99 as subplots.
    """
    brokers = sorted(df['broker'].unique())
    qos_levels = sorted(df['qos'].unique())
    
    if len(brokers) == 0:
        print("WARN: No brokers found for combined plot")
        return
    
    fig, axes = plt.subplots(1, 3, figsize=(18, max(6, len(brokers) * 0.6)), sharey=True)
    
    latency_cols = [('p50_ms', 'P50 Latency'), ('p95_ms', 'P95 Latency'), ('p99_ms', 'P99 Latency')]
    
    for ax_idx, (col, label) in enumerate(latency_cols):
        ax = axes[ax_idx]
        n_qos = len(qos_levels)
        bar_height = 0.8 / n_qos
        
        for i, qos in enumerate(qos_levels):
            qos_df = df[df['qos'] == qos]
            
            y_positions = []
            latencies = []
            for j, broker in enumerate(brokers):
                broker_qos_df = qos_df[qos_df['broker'] == broker]
                if not broker_qos_df.empty and pd.notna(broker_qos_df[col].iloc[0]):
                    y_positions.append(j + (i - n_qos/2 + 0.5) * bar_height)
                    latencies.append(broker_qos_df[col].iloc[0])
            
            if y_positions:
                ax.barh(
                    y_positions,
                    latencies,
                    height=bar_height * 0.9,
                    color=QOS_COLORS.get(qos, '#999999'),
                    label=QOS_LABELS.get(qos, f'QoS {qos}') if ax_idx == 0 else '',
                    edgecolor='white',
                    linewidth=0.5,
                )
        
        ax.set_xlabel('Latency (ms)')
        ax.set_title(label)
        ax.xaxis.grid(True, linestyle='--', alpha=0.7)
        ax.set_axisbelow(True)
    
    # Set y-axis labels on first subplot only
    axes[0].set_yticks(range(len(brokers)))
    axes[0].set_yticklabels(brokers)
    
    # Single legend for all subplots
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', ncol=3, bbox_to_anchor=(0.5, 1.02))
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    
    out_path = out_dir / 'qos_latency_combined.png'
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_qos_penalty(df: pd.DataFrame, out_dir: Path):
    """
    Plot QoS penalty: latency increase from QoS 0 baseline.
    Shows how much slower QoS 1/2 are compared to QoS 0 for each broker.
    """
    brokers = sorted(df['broker'].unique())
    
    # Need both QoS 0 and at least one other QoS level
    if 0 not in df['qos'].unique():
        print("WARN: No QoS 0 baseline for penalty plot")
        return
    
    fig, ax = plt.subplots(figsize=(12, max(6, len(brokers) * 0.8)))
    
    bar_data = []
    for broker in brokers:
        broker_df = df[df['broker'] == broker]
        qos0_df = broker_df[broker_df['qos'] == 0]
        
        if qos0_df.empty or pd.isna(qos0_df['p99_ms'].iloc[0]):
            continue
        
        baseline = qos0_df['p99_ms'].iloc[0]
        
        for qos in [1, 2]:
            qos_df = broker_df[broker_df['qos'] == qos]
            if not qos_df.empty and pd.notna(qos_df['p99_ms'].iloc[0]):
                latency = qos_df['p99_ms'].iloc[0]
                penalty = latency - baseline
                multiplier = latency / baseline if baseline > 0 else 0
                bar_data.append({
                    'broker': broker,
                    'qos': qos,
                    'penalty_ms': penalty,
                    'multiplier': multiplier,
                })
    
    if not bar_data:
        print("WARN: Insufficient data for penalty plot")
        return
    
    penalty_df = pd.DataFrame(bar_data)
    n_qos = 2  # Only QoS 1 and 2
    bar_height = 0.8 / n_qos
    
    for i, qos in enumerate([1, 2]):
        qos_data = penalty_df[penalty_df['qos'] == qos]
        
        y_positions = []
        penalties = []
        for j, broker in enumerate(brokers):
            broker_qos_data = qos_data[qos_data['broker'] == broker]
            if not broker_qos_data.empty:
                y_positions.append(j + (i - n_qos/2 + 0.5) * bar_height)
                penalties.append(broker_qos_data['penalty_ms'].iloc[0])
        
        if y_positions:
            ax.barh(
                y_positions,
                penalties,
                height=bar_height * 0.9,
                color=QOS_COLORS.get(qos, '#999999'),
                label=f'{QOS_LABELS[qos]} vs QoS 0',
                edgecolor='white',
                linewidth=0.5,
            )
            
            # Add multiplier labels
            for y, pen, data in zip(y_positions, penalties, qos_data.to_dict('records')):
                if data['broker'] in brokers:
                    mult = data['multiplier']
                    ax.text(
                        pen + ax.get_xlim()[1] * 0.01,
                        y,
                        f'+{pen:.1f}ms ({mult:.1f}×)',
                        va='center',
                        fontsize=8,
                    )
    
    ax.set_yticks(range(len(brokers)))
    ax.set_yticklabels(brokers)
    ax.set_xlabel('Latency Penalty vs QoS 0 (ms)')
    ax.set_title('QoS Latency Penalty (P99)')
    ax.legend(loc='lower right')
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    ax.axvline(x=0, color='black', linewidth=0.5)
    
    plt.tight_layout()
    
    out_path = out_dir / 'qos_latency_penalty.png'
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {out_path}")


def generate_html_report(out_dir: Path, summary_path: str):
    """Generate HTML gallery of all plots."""
    plots = sorted(out_dir.glob('*.png'))
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>QoS Comparison Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        .plot-container { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        img { max-width: 100%; height: auto; }
        .info { color: #666; font-size: 14px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>QoS Comparison Results</h1>
    <div class="info">
        <p>Summary CSV: <code>%s</code></p>
        <p>Generated: %s plots</p>
    </div>
""" % (summary_path, len(plots))
    
    for plot in plots:
        html += f"""
    <div class="plot-container">
        <h2>{plot.stem.replace('_', ' ').title()}</h2>
        <img src="{plot.name}" alt="{plot.stem}">
    </div>
"""
    
    html += """
</body>
</html>
"""
    
    html_path = out_dir / 'index.html'
    html_path.write_text(html)
    print(f"Saved: {html_path}")


def main():
    parser = argparse.ArgumentParser(description='Plot QoS comparison results')
    parser.add_argument('--summary', required=True, help='Path to summary.csv')
    parser.add_argument('--out-dir', required=True, help='Output directory for plots')
    args = parser.parse_args()
    
    # Load data
    df = load_summary(args.summary)
    
    if df.empty:
        print("ERROR: No valid QoS data in summary CSV")
        sys.exit(1)
    
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Loaded {len(df)} rows with brokers: {df['broker'].unique().tolist()}")
    print(f"QoS levels: {sorted(df['qos'].unique().tolist())}")
    
    # Generate individual latency plots
    plot_latency_bars(df, 'p50_ms', 'P50 Latency by Broker × QoS', out_dir / 'qos_p50_latency.png')
    plot_latency_bars(df, 'p95_ms', 'P95 Latency by Broker × QoS', out_dir / 'qos_p95_latency.png')
    plot_latency_bars(df, 'p99_ms', 'P99 Latency by Broker × QoS', out_dir / 'qos_p99_latency.png')
    
    # Combined plot
    plot_combined_latencies(df, out_dir)
    
    # QoS penalty plot
    plot_qos_penalty(df, out_dir)
    
    # HTML report
    generate_html_report(out_dir, args.summary)
    
    print(f"\nAll plots saved to: {out_dir}")


if __name__ == '__main__':
    main()
