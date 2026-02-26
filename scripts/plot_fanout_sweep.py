#!/usr/bin/env python3
"""
Plot fanout sweep results: throughput, latency, and loss vs subscriber count.

Usage:
    python3 scripts/plot_fanout_sweep.py --summary results/fanout_sweep_*/raw_data/summary.csv
    python3 scripts/plot_fanout_sweep.py --summary summary.csv --out-dir plots/
    python3 scripts/plot_fanout_sweep.py --summary summary.csv --metric throughput latency loss
"""

import argparse
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Style configuration
plt.style.use('seaborn-v0_8-whitegrid')
COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
MARKERS = ['o', 's', '^', 'D', 'v', '<', '>', 'p']


def load_summary(csv_path: str) -> pd.DataFrame:
    """Load and validate summary CSV."""
    df = pd.read_csv(csv_path)
    required_cols = ['subs', 'sub_tps']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


def plot_throughput_vs_subs(df: pd.DataFrame, out_dir: str, group_by: str = 'engine'):
    """Plot subscriber throughput vs number of subscribers."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    if group_by and group_by in df.columns:
        groups = df.groupby(group_by)
        for idx, (name, group) in enumerate(groups):
            group_sorted = group.sort_values('subs')
            ax.plot(group_sorted['subs'], group_sorted['sub_tps'], 
                   marker=MARKERS[idx % len(MARKERS)], 
                   color=COLORS[idx % len(COLORS)],
                   linewidth=2, markersize=8, label=str(name))
    else:
        df_sorted = df.sort_values('subs')
        ax.plot(df_sorted['subs'], df_sorted['sub_tps'], 
               marker='o', color=COLORS[0], linewidth=2, markersize=8)
    
    ax.set_xlabel('Number of Subscribers', fontsize=12)
    ax.set_ylabel('Throughput (msg/s)', fontsize=12)
    ax.set_title('Subscriber Throughput vs Subscriber Count', fontsize=14)
    
    # Use log scale if range is large
    subs_range = df['subs'].max() / max(df['subs'].min(), 1)
    if subs_range > 10:
        ax.set_xscale('log')
        ax.set_xticks(sorted(df['subs'].unique()))
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    
    if group_by and group_by in df.columns:
        ax.legend(title=group_by.capitalize(), loc='best')
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    for ext in ['png', 'pdf']:
        path = os.path.join(out_dir, f'throughput_vs_subs.{ext}')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Saved: {path}")
    plt.close(fig)


def plot_latency_vs_subs(df: pd.DataFrame, out_dir: str, group_by: str = 'engine'):
    """Plot P99 latency vs number of subscribers."""
    if 'p99_ms' not in df.columns:
        print("WARN: p99_ms column not found, skipping latency plot")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    if group_by and group_by in df.columns:
        groups = df.groupby(group_by)
        for idx, (name, group) in enumerate(groups):
            group_sorted = group.sort_values('subs')
            ax.plot(group_sorted['subs'], group_sorted['p99_ms'], 
                   marker=MARKERS[idx % len(MARKERS)], 
                   color=COLORS[idx % len(COLORS)],
                   linewidth=2, markersize=8, label=str(name))
    else:
        df_sorted = df.sort_values('subs')
        ax.plot(df_sorted['subs'], df_sorted['p99_ms'], 
               marker='o', color=COLORS[0], linewidth=2, markersize=8)
    
    ax.set_xlabel('Number of Subscribers', fontsize=12)
    ax.set_ylabel('P99 Latency (ms)', fontsize=12)
    ax.set_title('P99 Latency vs Subscriber Count', fontsize=14)
    
    subs_range = df['subs'].max() / max(df['subs'].min(), 1)
    if subs_range > 10:
        ax.set_xscale('log')
        ax.set_xticks(sorted(df['subs'].unique()))
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    
    if group_by and group_by in df.columns:
        ax.legend(title=group_by.capitalize(), loc='best')
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    for ext in ['png', 'pdf']:
        path = os.path.join(out_dir, f'latency_vs_subs.{ext}')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Saved: {path}")
    plt.close(fig)


def plot_loss_vs_subs(df: pd.DataFrame, out_dir: str, group_by: str = 'engine'):
    """Plot message loss percentage vs number of subscribers."""
    if 'loss_pct' not in df.columns:
        print("WARN: loss_pct column not found, skipping loss plot")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    if group_by and group_by in df.columns:
        groups = df.groupby(group_by)
        for idx, (name, group) in enumerate(groups):
            group_sorted = group.sort_values('subs')
            ax.plot(group_sorted['subs'], group_sorted['loss_pct'], 
                   marker=MARKERS[idx % len(MARKERS)], 
                   color=COLORS[idx % len(COLORS)],
                   linewidth=2, markersize=8, label=str(name))
    else:
        df_sorted = df.sort_values('subs')
        ax.plot(df_sorted['subs'], df_sorted['loss_pct'], 
               marker='o', color=COLORS[0], linewidth=2, markersize=8)
    
    ax.set_xlabel('Number of Subscribers', fontsize=12)
    ax.set_ylabel('Message Loss (%)', fontsize=12)
    ax.set_title('Message Loss vs Subscriber Count', fontsize=14)
    
    subs_range = df['subs'].max() / max(df['subs'].min(), 1)
    if subs_range > 10:
        ax.set_xscale('log')
        ax.set_xticks(sorted(df['subs'].unique()))
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    
    ax.set_ylim(bottom=0)
    
    if group_by and group_by in df.columns:
        ax.legend(title=group_by.capitalize(), loc='best')
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    for ext in ['png', 'pdf']:
        path = os.path.join(out_dir, f'loss_vs_subs.{ext}')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Saved: {path}")
    plt.close(fig)


def plot_combined(df: pd.DataFrame, out_dir: str, group_by: str = 'engine'):
    """Plot throughput, latency, and loss in a combined 3-panel figure."""
    has_latency = 'p99_ms' in df.columns
    has_loss = 'loss_pct' in df.columns
    
    n_plots = 1 + int(has_latency) + int(has_loss)
    fig, axes = plt.subplots(1, n_plots, figsize=(5 * n_plots, 5))
    if n_plots == 1:
        axes = [axes]
    
    ax_idx = 0
    
    # Throughput
    ax = axes[ax_idx]
    if group_by and group_by in df.columns:
        groups = df.groupby(group_by)
        for idx, (name, group) in enumerate(groups):
            group_sorted = group.sort_values('subs')
            ax.plot(group_sorted['subs'], group_sorted['sub_tps'], 
                   marker=MARKERS[idx % len(MARKERS)], 
                   color=COLORS[idx % len(COLORS)],
                   linewidth=2, markersize=6, label=str(name))
    else:
        df_sorted = df.sort_values('subs')
        ax.plot(df_sorted['subs'], df_sorted['sub_tps'], 
               marker='o', color=COLORS[0], linewidth=2, markersize=6)
    ax.set_xlabel('Subscribers')
    ax.set_ylabel('Throughput (msg/s)')
    ax.set_title('Throughput')
    ax.grid(True, alpha=0.3)
    ax_idx += 1
    
    # Latency
    if has_latency:
        ax = axes[ax_idx]
        if group_by and group_by in df.columns:
            for idx, (name, group) in enumerate(df.groupby(group_by)):
                group_sorted = group.sort_values('subs')
                ax.plot(group_sorted['subs'], group_sorted['p99_ms'], 
                       marker=MARKERS[idx % len(MARKERS)], 
                       color=COLORS[idx % len(COLORS)],
                       linewidth=2, markersize=6, label=str(name))
        else:
            df_sorted = df.sort_values('subs')
            ax.plot(df_sorted['subs'], df_sorted['p99_ms'], 
                   marker='o', color=COLORS[0], linewidth=2, markersize=6)
        ax.set_xlabel('Subscribers')
        ax.set_ylabel('P99 Latency (ms)')
        ax.set_title('Latency')
        ax.grid(True, alpha=0.3)
        ax_idx += 1
    
    # Loss
    if has_loss:
        ax = axes[ax_idx]
        if group_by and group_by in df.columns:
            for idx, (name, group) in enumerate(df.groupby(group_by)):
                group_sorted = group.sort_values('subs')
                ax.plot(group_sorted['subs'], group_sorted['loss_pct'], 
                       marker=MARKERS[idx % len(MARKERS)], 
                       color=COLORS[idx % len(COLORS)],
                       linewidth=2, markersize=6, label=str(name))
        else:
            df_sorted = df.sort_values('subs')
            ax.plot(df_sorted['subs'], df_sorted['loss_pct'], 
                   marker='o', color=COLORS[0], linewidth=2, markersize=6)
        ax.set_xlabel('Subscribers')
        ax.set_ylabel('Loss (%)')
        ax.set_title('Message Loss')
        ax.set_ylim(bottom=0)
        ax.grid(True, alpha=0.3)
    
    # Add legend to first plot
    if group_by and group_by in df.columns:
        axes[0].legend(title=group_by.capitalize(), loc='best', fontsize=8)
    
    plt.tight_layout()
    
    for ext in ['png', 'pdf']:
        path = os.path.join(out_dir, f'fanout_combined.{ext}')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Saved: {path}")
    plt.close(fig)


def plot_pub_scalability(df: pd.DataFrame, out_dir: str):
    """Plot throughput vs publishers (if multiple pub counts exist)."""
    if 'pubs' not in df.columns or df['pubs'].nunique() <= 1:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Group by subs count
    for idx, (subs, group) in enumerate(df.groupby('subs')):
        group_sorted = group.sort_values('pubs')
        ax.plot(group_sorted['pubs'], group_sorted['sub_tps'], 
               marker=MARKERS[idx % len(MARKERS)], 
               color=COLORS[idx % len(COLORS)],
               linewidth=2, markersize=8, label=f'{subs} subs')
    
    ax.set_xlabel('Number of Publishers', fontsize=12)
    ax.set_ylabel('Throughput (msg/s)', fontsize=12)
    ax.set_title('Throughput vs Publisher Count', fontsize=14)
    ax.legend(title='Subscribers', loc='best')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    for ext in ['png', 'pdf']:
        path = os.path.join(out_dir, f'throughput_vs_pubs.{ext}')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Saved: {path}")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description='Plot fanout sweep results')
    parser.add_argument('--summary', required=True, help='Path to summary.csv')
    parser.add_argument('--out-dir', default=None, help='Output directory for plots')
    parser.add_argument('--metric', nargs='+', default=['throughput', 'latency', 'loss', 'combined'],
                       choices=['throughput', 'latency', 'loss', 'combined', 'pubs'],
                       help='Metrics to plot')
    parser.add_argument('--group-by', default='engine', help='Column to group by (default: engine)')
    args = parser.parse_args()
    
    # Load data
    df = load_summary(args.summary)
    print(f"Loaded {len(df)} rows from {args.summary}")
    print(f"Columns: {list(df.columns)}")
    print(f"Subscribers: {sorted(df['subs'].unique())}")
    if 'engine' in df.columns:
        print(f"Engines: {list(df['engine'].unique())}")
    
    # Setup output directory
    out_dir = args.out_dir
    if not out_dir:
        out_dir = os.path.join(os.path.dirname(args.summary), '..', 'plots')
    os.makedirs(out_dir, exist_ok=True)
    
    # Generate plots
    if 'throughput' in args.metric:
        plot_throughput_vs_subs(df, out_dir, args.group_by)
    
    if 'latency' in args.metric:
        plot_latency_vs_subs(df, out_dir, args.group_by)
    
    if 'loss' in args.metric:
        plot_loss_vs_subs(df, out_dir, args.group_by)
    
    if 'combined' in args.metric:
        plot_combined(df, out_dir, args.group_by)
    
    if 'pubs' in args.metric:
        plot_pub_scalability(df, out_dir)
    
    print(f"\nAll plots saved to: {out_dir}")


if __name__ == '__main__':
    main()
