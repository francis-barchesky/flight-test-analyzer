"""
reanalyze_weight_sorties.py — targeted re-analyze pass for the inertia-estimation dataset.

Scope:
- Only sorties whose existing analysis_*.json has weight signals in headers
  (proxy for "the weight datagroup ZIP was included in the download")
- Only sorties that still have ZIPs on disk (need to re-analyze from raw)
- Backs up existing analysis_*.json + analysis_*_hires.json to .bak before
  deleting, so the fresh run does a clean write (bypasses the _write_or_merge_result
  protection path)
- Runs analyze_iads.py in parallel across N sorties.

Usage:
    python reanalyze_weight_sorties.py --dry-run    # preview
    python reanalyze_weight_sorties.py              # execute
    python reanalyze_weight_sorties.py --parallel 4 # override parallelism
"""
import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.abspath(__file__))
WEIGHT_SIGS = {"lt_fuel_wt", "rt_fuel_wt", "total_fuel",
               "ZFW_updated", "ZFWCG_updated",
               "CG_current", "WT_current"}


def has_weight(analysis_path):
    """Return True if this analysis JSON has weight signals in its headers."""
    try:
        with open(analysis_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return False
    headers = data.get("headers", [])
    suffixes = {h.rstrip(".").split(".")[-1] for h in headers}
    return bool(WEIGHT_SIGS & suffixes)


def find_targets():
    """Return list of sortie dirs that (a) have weight signals in headers and (b) have ZIPs on disk."""
    targets = []
    all_analyses = sorted(glob.glob(os.path.join(ROOT, "*_N208B", "analysis_*.json")))
    all_analyses = [p for p in all_analyses if "_hires" not in p]
    for path in all_analyses:
        if not has_weight(path):
            continue
        sortie_dir = os.path.dirname(path)
        zips = glob.glob(os.path.join(sortie_dir, "*.zip"))
        if not zips:
            continue
        targets.append((sortie_dir, path, zips))
    return targets


def backup_and_delete(analysis_path):
    """Rename analysis_*.json → .bak and analysis_*_hires.json → .bak so the
    fresh run does a full write instead of falling into the merge path."""
    hires_path = analysis_path.replace(".json", "_hires.json")
    for p in (analysis_path, hires_path):
        if os.path.exists(p):
            shutil.move(p, p + ".bak")


def resolve_trace_graph(sortie_name, tg_map):
    """Match sortie name against trace_graph_map (same rules as run_batch.py)."""
    def _tg_match(key, sname):
        if "_" in key:
            kparts = key.split("_", 1)
            sparts = sname.split("_")
            return sparts[0] == kparts[0] and sparts[-1] == kparts[1]
        return key in sname
    for k, v in tg_map.items():
        if _tg_match(k, sortie_name):
            return v
    return None


def run_one(sortie_dir, analysis_path, cfg, workers, dry_run):
    """Run analyze_iads.py on a single sortie dir. Returns result dict."""
    name = os.path.basename(sortie_dir)
    tg = resolve_trace_graph(name, cfg.get("trace_graph_map", {}))
    cmd = [
        sys.executable,
        os.path.join(ROOT, "analyze_iads.py"),
        sortie_dir,
        "--out",          "analysis.json",
        "--trigger",      cfg["trigger"],
        "--trigger-from", str(cfg["trigger_from"]),
        "--trigger-to",   str(cfg["trigger_to"]),
        "--workers",      str(workers),
        "--plot-signals", cfg["plot_signals"],
    ]
    if cfg.get("exclude_zip_patterns"):
        cmd += ["--exclude-zips", ",".join(cfg["exclude_zip_patterns"])]
    if tg:
        cmd += ["--trace-graph", tg]

    if dry_run:
        print(f"  [dry-run] {name}  (tg={tg})", flush=True)
        return {"sortie": name, "status": "dry-run", "elapsed_s": 0}

    backup_and_delete(analysis_path)
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0

    if proc.returncode != 0:
        print(f"  ERROR  {name}  exit={proc.returncode}  ({elapsed:.0f}s)", flush=True)
        if proc.stderr:
            print(proc.stderr[:1000], flush=True)
        return {"sortie": name, "status": "error", "elapsed_s": round(elapsed, 1)}

    # Quick post-check: new hires exists and has flight_plots with new signals
    hires_path = analysis_path.replace(".json", "_hires.json")
    ok = os.path.exists(hires_path)
    print(f"  OK  {name}  ({elapsed:.0f}s, hires={'yes' if ok else 'MISSING'})", flush=True)
    return {"sortie": name, "status": "ok" if ok else "no-hires", "elapsed_s": round(elapsed, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--parallel", type=int, default=None,
                    help="Number of sorties in parallel (default: cfg.parallel_sorties or 2)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Limit to first N sorties (for testing)")
    args = ap.parse_args()

    cfg = json.load(open(os.path.join(ROOT, "batch_config.json"), encoding="utf-8"))
    targets = find_targets()
    print(f"Found {len(targets)} weight-enabled sorties with ZIPs present.")
    if args.limit:
        targets = targets[:args.limit]
        print(f"  --limit={args.limit} -> processing {len(targets)}")

    parallel = args.parallel or cfg.get("parallel_sorties", 2) or 2
    # Prefer CPU count as the total worker budget so a big machine actually gets used.
    total_workers = max(cfg.get("workers", 8) or 8, os.cpu_count() or 8)
    workers_per_sortie = max(1, total_workers // max(1, parallel))
    print(f"Parallel sorties: {parallel}, workers per sortie: {workers_per_sortie}")

    if args.dry_run:
        print("\nDRY RUN — no changes will be made.\n")

    t_batch = time.perf_counter()
    results = []

    if parallel > 1:
        with ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {ex.submit(run_one, sd, ap_, cfg, workers_per_sortie, args.dry_run): sd
                       for sd, ap_, _ in targets}
            for f in as_completed(futures):
                results.append(f.result())
                done = sum(1 for r in results if r["status"] == "ok")
                errs = sum(1 for r in results if r["status"] == "error")
                print(f"  progress: {len(results)}/{len(targets)}  ok={done}  err={errs}", flush=True)
    else:
        for sd, ap_, _ in targets:
            results.append(run_one(sd, ap_, cfg, workers_per_sortie, args.dry_run))

    elapsed = time.perf_counter() - t_batch
    ok = sum(1 for r in results if r["status"] == "ok")
    err = sum(1 for r in results if r["status"] == "error")
    dr = sum(1 for r in results if r["status"] == "dry-run")
    print()
    print(f"=== Batch complete ({elapsed:.0f}s) ===")
    print(f"  ok:    {ok}")
    print(f"  err:   {err}")
    print(f"  dry:   {dr}")
    print(f"  other: {len(results) - ok - err - dr}")

    sys.exit(1 if err else 0)


if __name__ == "__main__":
    main()
