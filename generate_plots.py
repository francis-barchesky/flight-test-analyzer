#!/usr/bin/env python3
"""
generate_plots.py — Generate summary plots for the proposal appendix.

Produces four PDF figures in a plots/ subdirectory:
  plot_correlations.pdf   — horizontal bar chart of top 20 scored signals
  plot_timing.pdf         — scatter: mean dt (samples) vs score, by cluster
  plot_hop_dist.pdf       — histogram of upstream TestPoints by hop depth
  plot_sortie_times.pdf   — sorted bar chart of per-sortie processing times

Usage:
    python generate_plots.py
"""

import json
import os
import glob

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT  = os.path.join(ROOT, 'plots')
os.makedirs(OUT, exist_ok=True)

# Colour scheme matching the LaTeX table
C_HIGHLIGHT = '#DCEDCA'   # green  — near-simultaneous
C_PRECURSOR = '#FFF3CD'   # yellow — precursor
C_CONTEXT   = '#CFE2FF'   # blue   — early context
C_HL_EDGE   = '#6aaa3a'
C_PR_EDGE   = '#d4a017'
C_CT_EDGE   = '#3a7aaa'

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 9,
    'axes.titlesize': 10,
    'axes.labelsize': 9,
    'xtick.labelsize': 8,
    'ytick.labelsize': 8,
    'figure.dpi': 150,
})


def _cluster(dt_s):
    if dt_s < -1.0:
        return 'context'
    if dt_s < -0.05:
        return 'precursor'
    return 'highlight'


def _colors(dt_s):
    c = _cluster(dt_s)
    if c == 'context':
        return C_CONTEXT, C_CT_EDGE
    if c == 'precursor':
        return C_PRECURSOR, C_PR_EDGE
    return C_HIGHLIGHT, C_HL_EDGE


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

with open(os.path.join(ROOT, 'fault_correlations.json')) as f:
    corr_data = json.load(f)

with open(os.path.join(ROOT, 'upstream_afcsCapable.json')) as f:
    upstream_data = json.load(f)

correlations = corr_data['correlations'][:20]
n_episodes   = corr_data['n_episodes']
n_sorties    = corr_data['n_sorties']

# ---------------------------------------------------------------------------
# Plot 1 — Horizontal bar chart: top 20 signals by score
# ---------------------------------------------------------------------------

signals = [c['signal'] for c in correlations]
scores  = [c['score']  for c in correlations]
dts     = [c['mean_dt_s'] for c in correlations]
face_colors = [_colors(d)[0] for d in dts]
edge_colors = [_colors(d)[1] for d in dts]

fig, ax = plt.subplots(figsize=(7, 5.5))
y = np.arange(len(signals))
bars = ax.barh(y, scores, color=face_colors, edgecolor=edge_colors, linewidth=0.7)
ax.set_yticks(y)
ax.set_yticklabels([f'\\texttt{{{s}}}' if False else s for s in signals],
                   fontfamily='monospace', fontsize=7.5)
ax.invert_yaxis()
ax.set_xlabel('Correlation Score')
ax.set_title(
    f'Top 20 Correlated Signals — AFCS Disengagement\n'
    f'({n_episodes} episodes, {n_sorties} sorties, window $-5$s to $+1$s)',
    fontsize=9
)
ax.axvline(0, color='black', linewidth=0.5)
ax.grid(axis='x', linestyle='--', linewidth=0.4, alpha=0.6)

patches = [
    mpatches.Patch(facecolor=C_HIGHLIGHT, edgecolor=C_HL_EDGE, label='Near-simultaneous ($\\Delta t \\approx 0$)'),
    mpatches.Patch(facecolor=C_PRECURSOR, edgecolor=C_PR_EDGE, label='Precursor ($\\Delta t < -50$ms)'),
    mpatches.Patch(facecolor=C_CONTEXT,   edgecolor=C_CT_EDGE, label='Early context ($\\Delta t < -1$s)'),
]
ax.legend(handles=patches, fontsize=7.5, loc='lower right')
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'plot_correlations.pdf'), bbox_inches='tight')
plt.close(fig)
print('  plot_correlations.pdf')

# ---------------------------------------------------------------------------
# Plot 2 — Scatter: mean dt (samples) vs score, sized by frequency
# ---------------------------------------------------------------------------

fig, ax = plt.subplots(figsize=(6, 4))
for c in correlations:
    fc, ec = _colors(c['mean_dt_s'])
    size = 120 * c['frequency'] + 20
    ax.scatter(c['mean_dt_samples'], c['score'],
               s=size, color=fc, edgecolors=ec, linewidth=0.8, zorder=3)
    ax.annotate(c['signal'], (c['mean_dt_samples'], c['score']),
                fontsize=6, fontfamily='monospace',
                xytext=(4, 2), textcoords='offset points')

ax.axvline(0, color='grey', linewidth=0.8, linestyle='--', zorder=1)
ax.set_xlabel('Mean $\\Delta t$ (samples, 40 Hz clock)')
ax.set_ylabel('Correlation Score')
ax.set_title(
    'Timing vs Score — AFCS Fault Precursors\n'
    '(bubble size proportional to episode frequency)',
    fontsize=9
)
ax.grid(linestyle='--', linewidth=0.4, alpha=0.5)

# Phase annotations
ax.axvspan(-80, -55, alpha=0.06, color=C_CT_EDGE, label='Phase 1 (torque, ~$-$65 smp)')
ax.axvspan(-25,  -1, alpha=0.06, color=C_PR_EDGE, label='Phase 2 (cmd path, $-$4 to $-$20 smp)')
ax.axvspan(  0,   5, alpha=0.06, color=C_HL_EDGE, label='Phase 3 (health/monitor, 0$-$3 smp)')
ax.legend(fontsize=7, loc='upper left')
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'plot_timing.pdf'), bbox_inches='tight')
plt.close(fig)
print('  plot_timing.pdf')

# ---------------------------------------------------------------------------
# Plot 3 — Histogram: upstream TestPoints by hop depth
# ---------------------------------------------------------------------------

upstream = upstream_data.get('upstream', [])
hops = [u['hop'] for u in upstream]
max_hop = max(hops) if hops else 15
hop_counts = [hops.count(h) for h in range(1, max_hop + 1)]
cumulative  = np.cumsum(hop_counts)

fig, ax1 = plt.subplots(figsize=(6, 3.8))
ax2 = ax1.twinx()

x = np.arange(1, max_hop + 1)
ax1.bar(x, hop_counts, color='#4c8cbf', edgecolor='#2a5f8f', linewidth=0.6,
        alpha=0.85, label='Signals at hop')
ax2.plot(x, cumulative, color='#c0392b', linewidth=1.5, marker='o',
         markersize=3, label='Cumulative')
ax2.axhline(len(upstream), color='#c0392b', linewidth=0.8, linestyle='--', alpha=0.5)

ax1.set_xlabel('Hop Depth')
ax1.set_ylabel('TestPoint Signals at Depth', color='#4c8cbf')
ax2.set_ylabel('Cumulative Signals', color='#c0392b')
ax1.set_xticks(x)
ax1.tick_params(axis='y', labelcolor='#4c8cbf')
ax2.tick_params(axis='y', labelcolor='#c0392b')
ax1.set_title(
    f'Upstream TestPoint Distribution by Hop Depth\n'
    f'(signal: afcsCapable, total: {len(upstream):,} signals)',
    fontsize=9
)
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=7.5, loc='center right')
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'plot_hop_dist.pdf'), bbox_inches='tight')
plt.close(fig)
print('  plot_hop_dist.pdf')

# ---------------------------------------------------------------------------
# Plot 4 — Per-sortie processing times (sorted)
# ---------------------------------------------------------------------------

json_paths = sorted(
    p for p in glob.glob(f'{ROOT}/**/analysis_*.json', recursive=True)
    if '_hires' not in p
)

sortie_times = []
for path in json_paths:
    try:
        with open(path) as f:
            d = json.load(f)
        t = d.get('processing_time_s')
        if t is not None:
            name = os.path.basename(os.path.dirname(path))
            sortie_times.append((name, float(t)))
    except Exception:
        pass

sortie_times.sort(key=lambda x: x[1])
names = [s[0] for s in sortie_times]
times = [s[1] for s in sortie_times]

fig, ax = plt.subplots(figsize=(8, 4.5))
colors = ['#e74c3c' if t > 300 else '#f39c12' if t > 100 else '#2ecc71' for t in times]
ax.bar(range(len(times)), times, color=colors, edgecolor='none', width=1.0)
ax.set_xlabel('Sortie (sorted by processing time)')
ax.set_ylabel('Processing Time (s)')
ax.set_title(
    f'Per-Sortie Analysis Time — N208B 2026 Flight Testing ({len(times)} sorties)\n'
    f'Median: {np.median(times):.0f}s  |  Max: {max(times):.0f}s  |  '
    f'Total (parallel): ~55 min',
    fontsize=9
)
ax.set_xticks([])
ax.axhline(np.median(times), color='#2c3e50', linewidth=1.0, linestyle='--',
           label=f'Median ({np.median(times):.0f}s)')
ax.grid(axis='y', linestyle='--', linewidth=0.4, alpha=0.5)

patches = [
    mpatches.Patch(color='#2ecc71', label='$\\leq$100s'),
    mpatches.Patch(color='#f39c12', label='100s–300s'),
    mpatches.Patch(color='#e74c3c', label='>300s'),
]
ax.legend(handles=patches + [plt.Line2D([0],[0],color='#2c3e50',linestyle='--',
          label=f'Median ({np.median(times):.0f}s)')],
          fontsize=7.5, loc='upper left')
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'plot_sortie_times.pdf'), bbox_inches='tight')
plt.close(fig)
print('  plot_sortie_times.pdf')

print(f'\nAll plots written to {OUT}/')
