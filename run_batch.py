#!/usr/bin/env python3
"""
run_batch.py — batch runner for analyze_iads.py

Discovers every sub-directory under data_root that contains ZIP files,
runs analyze_iads.py on each, then (optionally) deletes the source ZIPs
so only the analysis JSON is retained.

Usage:
    python run_batch.py                        # uses batch_config.json in same dir
    python run_batch.py my_config.json         # explicit config path
    python run_batch.py --dry-run              # show what would run without running it
"""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# ── Sortie name from filename ──────────────────────────────────────────────────

def sortie_from_filename(filename):
    """
    Extract a sortie tag from a ZIP filename.
    e.g.  AFCS_del3_v20260202_S107N208B_2.zip  ->  S107_2
          AFCS_del3_v20260202_S107N208B.zip     ->  S107
    Returns None if no sortie tag can be found.
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    # S = Sortie, G = Ground  (e.g. S107N208B_2, G034N208B)
    m = re.search(r'([SG]\d{2,5})[A-Z][A-Z0-9]*_(\d+)', name, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}_{m.group(2)}"
    m = re.search(r'([SG]\d{2,5})[A-Z][A-Z0-9]+', name, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


# ── Download organizer ─────────────────────────────────────────────────────────

def organize_downloads(data_root, dry_run=False):
    """
    Move every *.zip sitting directly in data_root into a per-sortie
    subdirectory.  Files whose sortie cannot be determined are left in place
    and reported.

    Returns (moved, skipped) counts.
    """
    flat_zips = sorted(glob.glob(os.path.join(data_root, "*.zip")))
    if not flat_zips:
        print("  No flat ZIPs to organize.")
        return 0, 0

    # Group by sortie first so we can report cleanly
    groups = {}
    unknown = []
    for zf in flat_zips:
        sortie = sortie_from_filename(zf)
        if sortie:
            groups.setdefault(sortie, []).append(zf)
        else:
            unknown.append(zf)

    print(f"  {len(flat_zips)} ZIPs -> {len(groups)} sortie(s)")
    moved = 0

    for sortie in sorted(groups):
        dest_dir = os.path.join(data_root, sortie)
        files    = groups[sortie]
        print(f"    {sortie}/  ({len(files)} files)")
        for zf in files:
            dest = os.path.join(dest_dir, os.path.basename(zf))
            if dry_run:
                print(f"      [dry-run] {os.path.basename(zf)}")
            else:
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(zf, dest)
            moved += 1

    if unknown:
        print(f"  WARNING: {len(unknown)} file(s) skipped (no sortie tag in name):")
        for zf in unknown:
            print(f"    {os.path.basename(zf)}")

    return moved, len(unknown)


# ── Config loading ─────────────────────────────────────────────────────────────

def load_config(path):
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    # Strip _comment keys
    return {k: v for k, v in raw.items() if not k.startswith("_")}


# ── Sortie discovery ───────────────────────────────────────────────────────────

def find_sortie_dirs(data_root):
    """
    Return every immediate sub-directory of data_root that contains at least
    one ZIP file, sorted by directory name.
    """
    dirs = []
    try:
        entries = sorted(os.scandir(data_root), key=lambda e: e.name)
    except FileNotFoundError:
        print(f"ERROR: data_root not found: {data_root}")
        sys.exit(1)

    for entry in entries:
        if not entry.is_dir():
            continue
        zips = glob.glob(os.path.join(entry.path, "*.zip"))
        if zips:
            dirs.append(entry.path)
    return dirs


def find_analysis_json(sortie_dir):
    """
    Return the path of an existing analysis JSON in sortie_dir, or None.
    Matches analysis_*.json or analysis.json.
    """
    candidates = glob.glob(os.path.join(sortie_dir, "analysis*.json"))
    if candidates:
        # Prefer the most recently modified
        return max(candidates, key=os.path.getmtime)
    return None


# ── ZIP cleanup ────────────────────────────────────────────────────────────────

def delete_zips(sortie_dir, dry_run=False):
    zips = glob.glob(os.path.join(sortie_dir, "*.zip"))
    if not zips:
        return 0
    total_mb = sum(os.path.getsize(z) for z in zips) / 1e6
    for z in zips:
        if dry_run:
            print(f"    [dry-run] would delete: {os.path.basename(z)}")
        else:
            os.remove(z)
            print(f"    deleted: {os.path.basename(z)}")
    if not dry_run:
        print(f"    freed {total_mb:.0f} MB")
    return len(zips)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Batch runner for analyze_iads.py")
    ap.add_argument("config", nargs="?", default="batch_config.json",
                    help="Path to batch config JSON (default: batch_config.json)")
    ap.add_argument("--organize", action="store_true",
                    help="Move flat ZIPs in data_root into per-sortie subdirs before analyzing")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be run without executing anything")
    ap.add_argument("--parallel-sorties", type=int, default=None,
                    help="Analyze N sorties concurrently (overrides config parallel_sorties)")
    ap.add_argument("--status", action="store_true",
                    help="Show completion status of all sorties and exit")
    args = ap.parse_args()

    config_path = os.path.abspath(args.config)
    cfg = load_config(config_path)
    config_dir = os.path.dirname(config_path)

    # Resolve script path relative to config location
    script = cfg.get("script", "analyze_iads.py")
    if not os.path.isabs(script):
        script = os.path.join(config_dir, script)
    if not os.path.exists(script):
        print(f"ERROR: analyze_iads.py not found at: {script}")
        sys.exit(1)

    # Resolve data_root relative to config location
    data_root = cfg.get("data_root", ".")
    if not os.path.isabs(data_root):
        data_root = os.path.normpath(os.path.join(config_dir, data_root))

    if args.status:
        sortie_dirs = find_sortie_dirs(data_root)
        done_names = []
        pending_names = []
        no_ep_names = []

        for sd in sortie_dirs:
            name = os.path.basename(sd)
            j = find_analysis_json(sd)
            if j:
                try:
                    with open(j, encoding="utf-8") as f:
                        d = json.load(f)
                    if not d.get("episodes"):
                        no_ep_names.append(name)
                except Exception:
                    pass
                done_names.append(name)
            else:
                pending_names.append(name)

        total = len(sortie_dirs)
        pct   = int(100 * len(done_names) / total) if total else 0

        def _wrap(names, indent=18, width=100):
            """Wrap a list of names into continuation lines."""
            if not names:
                return "(none)"
            lines, cur = [], ""
            for n in names:
                token = n + "  "
                if cur and len(indent * " " + cur + token) > width:
                    lines.append(cur.rstrip())
                    cur = token
                else:
                    cur += token
            if cur:
                lines.append(cur.rstrip())
            pad = " " * indent
            return ("\n" + pad).join(lines)

        print(f"\nStatus  {data_root}  ({total} sortie(s) with ZIPs)\n")
        print(f"  DONE    ({len(done_names)}):  {_wrap(done_names)}")
        print(f"  PENDING ({len(pending_names)}):  {_wrap(pending_names)}")
        if no_ep_names:
            print(f"  0 eps   ({len(no_ep_names)}):  {_wrap(no_ep_names)}")
        print(f"\n  {len(done_names)}/{total} done ({pct}%)  |  {len(pending_names)} pending  |  {len(no_ep_names)} with 0 episodes\n")
        sys.exit(0)

    output_dir     = cfg.get("output_dir")
    trigger        = cfg.get("trigger", "afcsCapable")
    trigger_from   = cfg.get("trigger_from", 1.0)
    trigger_to     = cfg.get("trigger_to", 0.0)
    workers          = cfg.get("workers", 0)
    plot_signals     = cfg.get("plot_signals", "radAltVoted,gndSpdVoted")
    skip_existing    = cfg.get("skip_existing", True)
    delete_after     = cfg.get("delete_zips_after", True)
    parallel_sorties = args.parallel_sorties or cfg.get("parallel_sorties", 1)
    detected_cores   = os.cpu_count() or 1
    workers          = workers or detected_cores

    if output_dir and not os.path.isabs(output_dir):
        output_dir = os.path.normpath(os.path.join(config_dir, output_dir))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # ── Organize flat downloads into sortie subdirs ────────────────────────────
    if args.organize:
        print("=== Organizing downloads ===")
        moved, skipped = organize_downloads(data_root, dry_run=args.dry_run)
        if not args.dry_run:
            print(f"  Moved {moved} file(s) into sortie subdirectories")
        print()

    sortie_dirs = find_sortie_dirs(data_root)

    # Workers are divided across parallel sorties so total CPU usage stays bounded
    workers_per_sortie = max(1, workers // max(1, parallel_sorties))

    dry_tag = "  *** DRY RUN ***" if args.dry_run else ""
    auto_tag = f" (auto/{detected_cores})" if not cfg.get("workers") else ""
    par_tag = f"  |  parallel={parallel_sorties}  workers/sortie={workers_per_sortie}{auto_tag}" if parallel_sorties > 1 else f"  |  workers={workers_per_sortie}{auto_tag}"
    print(f"Batch  {data_root}  |  {len(sortie_dirs)} sortie(s)  |  trigger={trigger}{par_tag}{dry_tag}")

    if not sortie_dirs:
        print("No sortie directories with ZIP files found. Nothing to do.")
        sys.exit(0)

    results = []
    t_batch = time.perf_counter()
    n = len(sortie_dirs)

    def _run_sortie(i, sortie_dir):
        """Process one sortie. Returns a result dict. Thread-safe."""
        name = os.path.basename(sortie_dir)
        zips = glob.glob(os.path.join(sortie_dir, "*.zip"))
        existing_json = find_analysis_json(sortie_dir)

        out_dir  = output_dir or sortie_dir
        out_path = os.path.join(out_dir, "analysis.json")

        # ── Skip check ────────────────────────────────────────────────────────
        if skip_existing and existing_json:
            print(f"[{i}/{n}]  {name}  SKIP", flush=True)
            if delete_after and zips:
                delete_zips(sortie_dir, dry_run=args.dry_run)
            return {"sortie": name, "json": existing_json, "status": "skipped"}

        # ── Dry run ───────────────────────────────────────────────────────────
        cmd = [
            sys.executable, script,
            sortie_dir,
            "--out",          out_path,
            "--trigger",      trigger,
            "--trigger-from", str(trigger_from),
            "--trigger-to",   str(trigger_to),
            "--workers",      str(workers_per_sortie),
            "--plot-signals", plot_signals,
        ]
        if args.dry_run:
            print(f"[{i}/{n}]  {name}  [dry-run]", flush=True)
            return {"sortie": name, "json": None, "status": "dry-run"}

        # ── Run ───────────────────────────────────────────────────────────────
        print(f"[{i}/{n}]  {name}  ({len(zips)} ZIP(s))", flush=True)
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            elapsed = time.perf_counter() - t0
            # Print subprocess output atomically so parallel runs don't interleave
            if proc.stdout:
                print(proc.stdout, end='', flush=True)
            if proc.returncode != 0:
                if proc.stderr:
                    print(proc.stderr, end='', flush=True)
                raise subprocess.CalledProcessError(proc.returncode, cmd)

            written_json = find_analysis_json(out_dir)
            if delete_after:
                delete_zips(sortie_dir, dry_run=False)
            return {"sortie": name, "json": written_json, "status": "ok", "elapsed_s": round(elapsed, 1)}

        except subprocess.CalledProcessError as e:
            elapsed = time.perf_counter() - t0
            print(f"  ERROR  [{i}/{n}] {name}  exit {e.returncode}  ({elapsed:.1f}s)  — ZIPs NOT deleted", flush=True)
            return {"sortie": name, "json": None, "status": "error"}

    def _fmt_dur(secs):
        secs = int(secs)
        h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
        if h:   return f"{h}h {m:02d}m"
        if m:   return f"{m}m {s:02d}s"
        return f"{s}s"

    def _print_progress(results):
        done    = sum(1 for r in results if r["status"] in ("ok", "skipped"))
        errors  = sum(1 for r in results if r["status"] == "error")
        pending = n - len(results)
        pct     = int(100 * done / n) if n else 0
        bar_len = 30
        filled  = int(bar_len * done / n) if n else 0
        bar     = "#" * filled + "-" * (bar_len - filled)
        err_tag = f"  errors={errors}" if errors else ""

        # ETA: average elapsed of timed sorties, divided by parallelism
        timed = [r["elapsed_s"] for r in results if r.get("elapsed_s")]
        if timed and pending > 0:
            avg_s  = sum(timed) / len(timed)
            eta_s  = avg_s * pending / max(1, parallel_sorties)
            eta_dt = datetime.fromtimestamp(time.time() + eta_s).strftime("%H:%M")
            eta_tag = f"  ETA ~{_fmt_dur(eta_s)} ({eta_dt})"
        elif pending == 0:
            eta_tag = "  done"
        else:
            eta_tag = ""

        print(f"  [{bar}] {done}/{n} ({pct}%){eta_tag}{err_tag}", flush=True)

    if parallel_sorties > 1:
        with ThreadPoolExecutor(max_workers=parallel_sorties) as ex:
            futures = {ex.submit(_run_sortie, i, sd): i
                       for i, sd in enumerate(sortie_dirs, 1)}
            for f in as_completed(futures):
                results.append(f.result())
                _print_progress(results)
        # Re-sort results by original order for the manifest
        order = {os.path.basename(sd): i for i, sd in enumerate(sortie_dirs, 1)}
        results.sort(key=lambda r: order.get(r["sortie"], 0))
    else:
        for i, sortie_dir in enumerate(sortie_dirs, 1):
            results.append(_run_sortie(i, sortie_dir))
            _print_progress(results)

    # ── Manifest ───────────────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - t_batch
    manifest = {
        "config": config_path,
        "data_root": data_root,
        "trigger": trigger,
        "elapsed_s": round(total_elapsed, 1),
        "sorties": results,
    }
    manifest_path = os.path.join(output_dir or data_root, "batch_manifest.json")
    if not args.dry_run:
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    # ── Summary ────────────────────────────────────────────────────────────────
    ok      = sum(1 for r in results if r["status"] == "ok")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    errors  = sum(1 for r in results if r["status"] == "error")

    manifest_tag = f"  manifest={manifest_path}" if not args.dry_run else ""
    print(f"Batch done  {total_elapsed:.1f}s  |  ok={ok}  skipped={skipped}  errors={errors}{manifest_tag}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
