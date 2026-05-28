#!/usr/bin/env python3
"""
csv_cutter.py  --  Find columns by regex, cut to a new CSV, plot vs time.

Usage:
    python csv_cutter.py <csv_file> [pattern1 pattern2 ...]

    Patterns pre-filter the interactive column selector.
    Omit patterns to browse all columns.

Example:
    python csv_cutter.py data.csv "distancetorunway" "vertActiveEnum"
    python csv_cutter.py data.csv
"""

import re
import sys
import subprocess
from pathlib import Path

def _ensure(*packages):
    for pkg in packages:
        try:
            __import__(pkg.replace('-', '_'))
        except ImportError:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg])

_ensure('pandas', 'matplotlib', 'numpy', 'InquirerPy')

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from InquirerPy import inquirer
from InquirerPy.prompts.checkbox import CheckboxPrompt as _BaseCheckbox

class _CheckboxWithCount(_BaseCheckbox):
    """Checkbox prompt that appends a live selected-count to the prompt line."""
    def _get_prompt_message(self):
        msg = super()._get_prompt_message()
        if not self.status["answered"]:
            try:
                n = sum(1 for c in self.content_control.choices if getattr(c, 'enabled', False))
                msg.append(("class:instruction", f"  [{n} selected]"))
            except Exception:
                pass
        return msg


# -- Time parsing -------------------------------------------------------------

def _parse_elapsed(time_col):
    """Elapsed seconds from a pandas Series of time strings.
    Handles: DDD:HH:MM:SS.mmm  |  HH:MM:SS.mmm  |  plain numeric seconds.
    """
    out = []
    t0 = None
    for s in time_col:
        parts = str(s).split(':')
        try:
            if len(parts) == 4:
                sec = int(parts[0])*86400 + int(parts[1])*3600 + int(parts[2])*60 + float(parts[3])
            elif len(parts) == 3:
                sec = int(parts[0])*3600 + int(parts[1])*60 + float(parts[2])
            else:
                sec = float(s)
        except ValueError:
            sec = float('nan')
        if t0 is None:
            t0 = sec
        out.append(sec - t0)
    return out


# -- Column selector ----------------------------------------------------------

def _select_columns(candidates, title="Select columns to plot"):
    """Filter → checkbox loop. Re-filter as many times as needed; accumulated
    selections persist across rounds. Returns list of (col_idx, col_name)."""
    accumulated = {}  # col_name -> (ci, col)

    while True:
        n = len(accumulated)
        if n:
            filter_msg = f"Filter columns (substring/regex, Enter to finish with {n} selected):"
        else:
            filter_msg = "Filter columns (substring/regex, Enter for all):"

        filter_str = inquirer.text(message=filter_msg).execute().strip()

        if not filter_str:
            if accumulated:
                break                       # done
            filtered = candidates           # show everything on blank first pass
        else:
            try:
                filtered = [(i, col) for i, col in candidates
                            if re.search(filter_str, col, re.IGNORECASE)]
            except re.error:
                filtered = [(i, col) for i, col in candidates
                            if filter_str.lower() in col.lower()]
            if not filtered:
                print(f"  No matches for '{filter_str}'.")
                continue
            print(f"  {len(filtered)} match(es).")

        choices = [
            {'name': f"[{ci:5d}]  {col}", 'value': (ci, col),
             'enabled': col in accumulated}
            for ci, col in filtered
        ]
        selected = _CheckboxWithCount(
            message=f"{title} (Space=select, Enter=confirm):",
            choices=choices,
        ).execute()

        # Merge round: add newly selected, remove deselected from this pass
        for ci, col in filtered:
            if (ci, col) in selected:
                accumulated[col] = (ci, col)
            elif col in accumulated:
                del accumulated[col]

    if not accumulated:
        print("  Nothing selected.")
    return list(accumulated.values())


# -- Plot helper --------------------------------------------------------------

COLORS = [
    '#4CC9A0', '#D4924A', '#9490d4', '#E07070',
    '#6B62D4', '#C8922A', '#78D4E0', '#E0B878',
    '#A0D468', '#ED5565',
]

def _plot(t, df, sig_names, title):
    t_arr = np.asarray(t)
    n = len(sig_names)
    color_cycle = COLORS * (n // len(COLORS) + 1)

    fig, axes = plt.subplots(n, 1, figsize=(14, max(2.5 * n, 4)), sharex=True)
    if n == 1:
        axes = [axes]

    fig.patch.set_facecolor('#1a1a18')
    plot_lines = []
    for ax, sig, color in zip(axes, sig_names, color_cycle):
        ax.set_facecolor('#1e1e1c')
        line, = ax.plot(t, df[sig].tolist(), color=color, linewidth=0.8)
        plot_lines.append(line)
        short = sig.split('.')[-1]
        ax.set_ylabel(short, fontsize=8, color='#aaa', rotation=0, labelpad=8, ha='right', va='center')
        ax.set_title(sig, fontsize=6.5, color='#666', pad=2)
        ax.grid(True, alpha=0.2, color='#444')
        ax.tick_params(colors='#888', labelsize=7)
        for spine in ax.spines.values():
            spine.set_edgecolor('#333')

    axes[-1].set_xlabel('Elapsed time (s)', color='#aaa', fontsize=9)
    axes[-1].tick_params(axis='x', colors='#888', labelsize=7)
    fig.suptitle(title, fontsize=10, color='#ccc', y=1.0)
    plt.tight_layout()

    # Synchronized crosshair: vertical line + dot + datatip on every subplot
    vlines = [ax.axvline(color='#ffffff', linewidth=0.7, alpha=0.5, visible=False)
              for ax in axes]
    dots   = [ax.plot([], [], 'o', color=c, ms=5, zorder=5)[0]
              for ax, c in zip(axes, color_cycle)]
    anns   = [ax.annotate('', xy=(0, 0), xytext=(10, 10),
                          textcoords='offset points',
                          bbox=dict(boxstyle='round,pad=0.3', fc='#2a2a28', ec='#555', alpha=0.92),
                          color='#ddd', fontsize=8, visible=False)
              for ax in axes]

    def _show(tx, idx):
        for vl, dot, ann, line, sig in zip(vlines, dots, anns, plot_lines, sig_names):
            yval = float(line.get_ydata()[idx])
            vl.set_xdata([tx])
            vl.set_visible(True)
            dot.set_data([tx], [yval])
            ann.xy = (tx, yval)
            ann.set_text(f"t = {tx:.3f} s\n{sig.split('.')[-1]}: {yval:.4g}")
            ann.set_visible(True)
        fig.canvas.draw_idle()

    def _hide():
        for vl, dot, ann in zip(vlines, dots, anns):
            vl.set_visible(False)
            dot.set_data([], [])
            ann.set_visible(False)
        fig.canvas.draw_idle()

    def on_move(event):
        if event.xdata is None or event.inaxes not in axes:
            _hide()
            return
        idx = int(np.searchsorted(t_arr, event.xdata))
        idx = min(max(idx, 0), len(t_arr) - 1)
        _show(t_arr[idx], idx)

    fig.canvas.mpl_connect('motion_notify_event', on_move)
    plt.show()


# -- Core function ------------------------------------------------------------

def cut_and_plot(csv_path, *patterns, output=None, time_col='Time'):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return

    header = pd.read_csv(csv_path, nrows=0, encoding='utf-8-sig').columns.tolist()

    if time_col not in header:
        print(f"Time column '{time_col}' not found in header.")
        return

    # Build candidate list: all columns, or pre-filtered by patterns
    candidates = [(i, col) for i, col in enumerate(header) if col != time_col]
    if patterns:
        candidates = [(i, col) for i, col in candidates
                      if any(re.search(p, col, re.IGNORECASE) for p in patterns)]
        if not candidates:
            print(f"No columns matched patterns: {patterns}")
            return
        print(f"Pre-filtered to {len(candidates)} column(s) matching: {patterns}")
    else:
        print(f"{len(candidates)} columns available.")

    # Interactive fuzzy selector
    selected = _select_columns(candidates)
    if not selected:
        print("Nothing selected, exiting.")
        return

    sig_names = [col for _, col in selected]
    usecols = [time_col] + sig_names

    print(f"\nLoading {len(sig_names)} column(s) ...")
    df = pd.read_csv(csv_path, usecols=usecols, encoding='utf-8-sig')

    if output is None:
        output = csv_path.with_name(csv_path.stem + '_cut.csv')
    output = Path(output)
    print(f"Writing -> {output.name} ...")
    df.to_csv(output, index=False, encoding='utf-8')
    print(f"  Done. {output}")

    t = _parse_elapsed(df[time_col])

    # Initial plot
    _plot(t, df, sig_names, csv_path.stem)

    # Replot loop -- data stays in memory, no re-read needed
    while True:
        try:
            replot = _select_columns([(0, sig) for sig in sig_names], title="Replot which signals")
        except KeyboardInterrupt:
            break
        _plot(t, df, [col for _, col in replot], csv_path.stem)


# -- CLI entry point ----------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cut_and_plot(sys.argv[1], *sys.argv[2:])
