#!/usr/bin/env python3
"""Plot pitch-trim servo torque vs time for the four sorties whose peak exceeds 25 ft-lb.

Overlays radAltVoted on a secondary axis where available in the hires JSON.
Outputs pitch_trim_peaks.png next to this script.

Usage:
    py313 plot_pitch_trim_peaks.py            # default threshold 25
    py313 plot_pitch_trim_peaks.py 20         # different threshold
"""
import json
import os
import re
import sys

import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))

PTRIM_PATTERNS = [
    re.compile(r"pitchtrimservo.*trqRunningAvgVal$", re.I),
    re.compile(r"inservos.*srvPtTrqRunningAvgVal$", re.I),
    re.compile(r"Pitch_Trim_Servo\.PTSRV.*Torque_Running_Avg\.?$", re.I),
]
RADALT_RE = re.compile(r"radAltVoted$", re.I)

THRESHOLD = float(sys.argv[1]) if len(sys.argv) > 1 else 25.0
WINDOW_S = 30.0

NOT_APPLICABLE = {"S017_N208B", "S018_N208B"}


def find_main_json(sortie_dir):
    for f in os.listdir(sortie_dir):
        if f.startswith("analysis") and f.endswith(".json") and "_hires" not in f:
            return os.path.join(sortie_dir, f)
    return None


def find_hires_json(sortie_dir):
    for f in os.listdir(sortie_dir):
        if f.endswith("_hires.json"):
            return os.path.join(sortie_dir, f)
    return None


def collect_hits(threshold):
    hits = []
    for d in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, d)
        if not os.path.isdir(p):
            continue
        main_json = find_main_json(p)
        if not main_json:
            continue
        try:
            with open(main_json, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        peak = 0.0
        peak_sig = None
        peak_time = None
        for s in data.get("torque_stats") or []:
            sig = s.get("signal", "")
            pk = s.get("peak")
            if pk is None or not any(rx.search(sig) for rx in PTRIM_PATTERNS):
                continue
            v = abs(pk)
            if v > peak:
                peak = v
                peak_sig = sig
                peak_time = s.get("peak_time")
        if peak >= threshold:
            hits.append({
                "name": d,
                "peak": peak,
                "peak_sig": peak_sig,
                "peak_time": peak_time,
                "dir": p,
            })
    hits.sort(key=lambda h: -h["peak"])
    return hits


def extract_series(plots, predicate):
    out = []
    for sig, pts in plots.items():
        if not predicate(sig) or not pts:
            continue
        ts = [pt[0] for pt in pts]
        vs = [pt[1] for pt in pts]
        out.append((sig, ts, vs))
    return out


def short_label(sig):
    m = re.search(r"PTSRV_A429_TX_(\d)", sig)
    if m:
        return f"PTSRV TX_{m.group(1)}"
    return sig.split(".")[-1]


def plot_sortie(ax, hit):
    hires_path = find_hires_json(hit["dir"])
    if not hires_path:
        ax.set_title(f"{hit['name']}  (no hires JSON)")
        return
    with open(hires_path, encoding="utf-8") as fh:
        hires = json.load(fh)

    plots = {}
    for key in ("flight_plots", "takeoff_plots"):
        for sig, pts in (hires.get(key) or {}).items():
            plots.setdefault(sig, []).extend(pts)
    for sig in plots:
        plots[sig].sort(key=lambda p: p[0])

    ptrim_all = extract_series(plots, lambda s: any(rx.search(s) for rx in PTRIM_PATTERNS))
    ptrim = [(sig, ts, vs) for (sig, ts, vs) in ptrim_all if vs]
    radalt = extract_series(plots, lambda s: RADALT_RE.search(s))

    colors = ["#9A6BC4", "#5E3D8C", "#C798E0"]
    for i, (sig, ts, vs) in enumerate(ptrim):
        ax.plot(ts, vs, color=colors[i % len(colors)], lw=0.7, label=short_label(sig))

    ax.axhline(25, color="#E07070", lw=0.6, ls="--", alpha=0.7)
    ax.axhline(-25, color="#E07070", lw=0.6, ls="--", alpha=0.7)

    if hit["peak_time"] is not None:
        ax.plot([hit["peak_time"]], [hit["peak"]], "o", color="#E08040",
                ms=6, mec="white", mew=0.8, zorder=5, label=f"peak {hit['peak']:.2f}")
        ax.annotate(f"{hit['peak']:.2f} ft-lb",
                    xy=(hit["peak_time"], hit["peak"]),
                    xytext=(6, 4), textcoords="offset points",
                    fontsize=8, color="#E08040")

    ax.set_ylabel("torque (ft-lb)", fontsize=8)
    ax.tick_params(axis="both", labelsize=7)
    ax.grid(True, alpha=0.25)

    title = f"{hit['name']}  peak {hit['peak']:.2f} ft-lb"
    is_na = hit["name"] in NOT_APPLICABLE
    notes = []
    if is_na:
        notes.append("N/A scenario")
    if not ptrim:
        notes.append("no hires torque series")
    if not radalt:
        notes.append("no radAltVoted")
    if notes:
        title += "  (" + "; ".join(notes) + ")"

    if is_na:
        ax.text(0.5, 0.5, "NOT APPLICABLE\nSCENARIO",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=18, fontweight="bold",
                color="#E07070", alpha=0.35, rotation=15, zorder=10)
        for spine in ax.spines.values():
            spine.set_edgecolor("#E07070")
            spine.set_linewidth(1.2)

    if radalt:
        ax2 = ax.twinx()
        for sig, ts, vs in radalt:
            ax2.plot(ts, vs, color="#4CC9A0", lw=0.6, alpha=0.85, label="radAltVoted")
        ax2.set_ylabel("radAltVoted (ft)", fontsize=8, color="#4CC9A0")
        ax2.tick_params(axis="y", labelsize=7, colors="#4CC9A0")
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, fontsize=7, loc="upper right")
    else:
        if ax.get_legend_handles_labels()[1]:
            ax.legend(fontsize=7, loc="upper right")

    ax.set_title(title, fontsize=9)
    ax.set_xlabel(f"time (s, hires)  [±{WINDOW_S:.0f}s around peak]", fontsize=8)

    if hit["peak_time"] is not None:
        ax.set_xlim(hit["peak_time"] - WINDOW_S, hit["peak_time"] + WINDOW_S)


def main():
    hits = collect_hits(THRESHOLD)
    if not hits:
        print(f"No sorties found with pitch-trim peak > {THRESHOLD}")
        return

    n = len(hits)
    ncols = 2 if n > 1 else 1
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(13, 4 * nrows), squeeze=False)

    for ax, hit in zip(axes.flat, hits):
        plot_sortie(ax, hit)
    for ax in axes.flat[len(hits):]:
        ax.axis("off")

    fig.suptitle(
        f"Pitch-trim servo torque — {len(hits)} sorties with peak >= {THRESHOLD:.0f} ft-lb",
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))

    out = os.path.join(ROOT, "pitch_trim_peaks.png")
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")
    for h in hits:
        print(f"  {h['name']:<20} peak {h['peak']:.2f}  signal {h['peak_sig']}")


if __name__ == "__main__":
    main()
