#!/usr/bin/env python3
"""
patch_torque_stats.py — Back-fill torque_stats into existing analysis JSONs.

For each sortie directory that has a *_hires.json but whose main analysis JSON
is missing torque_stats, this script:
  1. Reads the hires flight_plots + takeoff_plots for torque signals
  2. Finds peak value + peak time per signal (min-max downsampling preserves peaks)
  3. Looks up active mode enums at peak time from mode_transitions in the main JSON
  4. Writes torque_stats back into the main JSON (in-place)

Usage:
    python patch_torque_stats.py                    # scans current directory
    python patch_torque_stats.py /path/to/data_root
    python patch_torque_stats.py --force            # overwrite even if torque_stats exists
"""

import json
import math
import os
import re
import sys

TORQUE_SIG_RE = re.compile(r'torq', re.I)
TORQUE_LIM_RE = re.compile(r'lim',  re.I)


def _modes_at_time(mode_trans, peak_t):
    modes = {}
    for sig_sfx in ("vertActiveEnum", "latActiveEnum", "atActiveEnum"):
        best = None
        for tr in mode_trans:
            if tr.get("signal", "").split(".")[-1] != sig_sfx:
                continue
            try:
                tt = float(tr["time"])
            except (ValueError, TypeError):
                continue
            if tt <= peak_t and (best is None or tt >= best["t"]):
                best = {"t": tt, "val": float(tr["to"])}
        if best is not None:
            modes[sig_sfx] = best["val"]
    return modes


def _torque_stats_from_plots(plots, mode_trans):
    """Compute torque_stats from a flight_plots or takeoff_plots dict."""
    stats = {}
    for sig, pts in plots.items():
        sfx = sig.split(".")[-1]
        if not TORQUE_SIG_RE.search(sfx) or TORQUE_LIM_RE.search(sfx):
            continue
        if not pts:
            continue
        peak_pt = max(pts, key=lambda p: p[1])
        peak_val, peak_t = peak_pt[1], peak_pt[0]
        if not math.isfinite(peak_val):
            continue
        # Keep the highest peak across flight_plots + takeoff_plots
        if sig not in stats or peak_val > stats[sig]["peak"]:
            stats[sig] = {
                "signal":    sig,
                "peak":      round(peak_val, 2),
                "peak_time": round(peak_t, 3),
                "modes":     _modes_at_time(mode_trans, peak_t),
            }
    return stats


def patch_sortie(sortie_dir, force=False):
    # Find main JSON
    main_json = None
    for f in os.listdir(sortie_dir):
        if f.startswith("analysis_") and f.endswith(".json") and "_hires" not in f:
            main_json = os.path.join(sortie_dir, f)
            break
    if main_json is None:
        return "skip", "no main JSON"

    hires_json = main_json.replace(".json", "_hires.json")
    if not os.path.exists(hires_json):
        return "skip", "no hires JSON"

    with open(main_json, encoding="utf-8") as f:
        main = json.load(f)

    if main.get("torque_stats") and not force:
        return "skip", "already has torque_stats"

    with open(hires_json, encoding="utf-8") as f:
        hires = json.load(f)

    mode_trans = main.get("mode_transitions", [])
    stats = {}

    for plots_key in ("flight_plots", "takeoff_plots"):
        plots = hires.get(plots_key) or {}
        for sig, entry in _torque_stats_from_plots(plots, mode_trans).items():
            if sig not in stats or entry["peak"] > stats[sig]["peak"]:
                stats[sig] = entry

    if not stats:
        return "skip", "no torque signals found in hires"

    main["torque_stats"] = list(stats.values())
    with open(main_json, "w", encoding="utf-8") as f:
        json.dump(main, f, separators=(",", ":"), allow_nan=False)

    return "patched", f"{len(stats)} signal(s)"


def main():
    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    data_root = args[0] if args else "."

    sortie_dirs = sorted([
        os.path.join(data_root, d)
        for d in os.listdir(data_root)
        if os.path.isdir(os.path.join(data_root, d)) and not d.startswith(".")
    ])

    patched = skipped = 0
    for sd in sortie_dirs:
        status, msg = patch_sortie(sd, force=force)
        name = os.path.basename(sd)
        if status == "patched":
            print(f"  patched  {name}  ({msg})")
            patched += 1
        else:
            print(f"  skipped  {name}  ({msg})")
            skipped += 1

    print(f"\nDone — {patched} patched, {skipped} skipped.")


if __name__ == "__main__":
    main()
