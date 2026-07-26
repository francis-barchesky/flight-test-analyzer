#!/usr/bin/env python3
"""
downsample_plots.py — post-process existing analysis JSON.gz files to cap
flight_plots and takeoff_plots to MAX_PTS points per signal using min-max
bucket downsampling (preserves peaks/transients).

Usage:
    python downsample_plots.py                        # all data roots in batch_config.json
    python downsample_plots.py --max-pts 4000         # default
    python downsample_plots.py --dry-run              # show what would change, don't write
    python downsample_plots.py data/ZKMLN             # single directory
"""

import argparse
import glob
import gzip
import json
import os
import sys

DEFAULT_MAX_PTS = 4000
PLOT_KEYS = ("flight_plots", "takeoff_plots")


def downsample_minmax(pts, max_pts):
    """Min-max bucket downsampling — preserves signal extremes within each bucket."""
    if len(pts) <= max_pts:
        return pts
    bucket_size = len(pts) / (max_pts // 2)
    out = []
    i = 0
    while i < len(pts) and len(out) < max_pts:
        j = min(len(pts), int(i + bucket_size))
        bucket = pts[i:j]
        if not bucket:
            break
        min_pt = min(bucket, key=lambda p: p[1])
        max_pt = max(bucket, key=lambda p: p[1])
        if min_pt[0] <= max_pt[0]:
            out.append(min_pt)
            if min_pt is not max_pt:
                out.append(max_pt)
        else:
            out.append(max_pt)
            if min_pt is not max_pt:
                out.append(min_pt)
        i = j
    return out


def process_file(path, max_pts, dry_run):
    dirname = os.path.dirname(path)
    basename = os.path.basename(path)
    out_path = os.path.join(dirname, "rs_" + basename)

    # Skip if rs_ file is already newer than the source (up to date)
    if os.path.exists(out_path) and os.path.getmtime(out_path) >= os.path.getmtime(path):
        return False

    if os.path.exists(out_path):
        print(f"  stale rs_ detected  {os.path.relpath(out_path)}  (regenerating)")

    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"  SKIP  {path}: {e}")
        return False

    changed = False
    for key in PLOT_KEYS:
        plots = data.get(key)
        if not isinstance(plots, dict):
            continue
        for sig, pts in plots.items():
            if len(pts) > max_pts:
                plots[sig] = downsample_minmax(pts, max_pts)
                changed = True

    if not changed:
        # Source has no oversized plots — stale rs_ is not needed; remove it
        if os.path.exists(out_path):
            if dry_run:
                print(f"  would delete stale  {os.path.relpath(out_path)}  (no plots to downsample)")
            else:
                os.remove(out_path)
                print(f"  deleted stale  {os.path.relpath(out_path)}")
        return False

    if dry_run:
        print(f"  would resample  {os.path.relpath(path)}  ->  rs_{basename}")
        return True

    try:
        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        print(f"  resampled  {os.path.relpath(out_path)}")
    except Exception as e:
        print(f"  ERROR writing {out_path}: {e}")
        return False
    return True


def find_analysis_files(root):
    return sorted(
        p for p in glob.glob(os.path.join(root, "**", "analysis*.json.gz"), recursive=True)
        if not os.path.basename(p).startswith("rs_")
    )


def main():
    ap = argparse.ArgumentParser(description="Downsample flight_plots/takeoff_plots in analysis JSON.gz files")
    ap.add_argument("roots", nargs="*", help="Data root directories (default: from batch_config.json)")
    ap.add_argument("--max-pts", type=int, default=DEFAULT_MAX_PTS)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default="batch_config.json")
    args = ap.parse_args()

    roots = args.roots
    if not roots:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.config)
        try:
            with open(config_path) as f:
                cfg = json.load(f)
            base = os.path.dirname(config_path)
            raw_map = cfg.get("data_root_map", {})
            if raw_map:
                roots = [r if os.path.isabs(r) else os.path.normpath(os.path.join(base, r))
                         for r in raw_map.values()]
            else:
                dr = cfg.get("data_root", ".")
                roots = [dr if os.path.isabs(dr) else os.path.normpath(os.path.join(base, dr))]
        except Exception as e:
            print(f"Could not read {config_path}: {e}")
            sys.exit(1)

    files = []
    for root in roots:
        found = find_analysis_files(root)
        print(f"{root}: {len(found)} analysis file(s)")
        files.extend(found)

    if not files:
        print("No analysis files found.")
        return

    print(f"\nDownsampling {len(files)} file(s) to {args.max_pts} pts/signal"
          + ("  [DRY RUN]" if args.dry_run else "") + "\n")

    changed = sum(process_file(p, args.max_pts, args.dry_run) for p in files)
    print(f"\nDone — {changed}/{len(files)} file(s) {'would be ' if args.dry_run else ''}resampled.")


if __name__ == "__main__":
    main()
