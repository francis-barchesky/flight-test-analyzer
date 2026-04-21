"""
generate_hires.py — regenerate *_hires.json files without re-running full analysis.

Reads existing analysis_*.json (for mode_transitions + windowing), re-reads ZIPs
to rebuild plot_series at _HIRES_MAX_PTS, and writes analysis_*_hires.json.

Usage:
  # All sorties under data_root from batch_config.json
  python generate_hires.py

  # One specific sortie directory
  python generate_hires.py S115_1_N208B

  # Force overwrite even if hires file already exists
  python generate_hires.py --force
"""
import os, sys, json, glob, tempfile, time
from multiprocessing.pool import ThreadPool

# Allow importing from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze_iads as ai


def _find_analysis_json(sortie_dir):
    """Return the analysis_*.json path in a sortie directory, or None."""
    candidates = sorted(glob.glob(os.path.join(sortie_dir, "analysis_*.json")))
    candidates = [c for c in candidates if "_hires" not in os.path.basename(c)]
    return candidates[0] if candidates else None


def _hires_path_for(analysis_path):
    root, ext = os.path.splitext(analysis_path)
    return f"{root}_hires{ext}"


def process_sortie(sortie_dir, exclude_patterns=None, plot_signals=None,
                   n_workers=8, force=False, quiet=False):
    """Generate a hires file for one sortie directory. Returns True on success."""
    analysis_path = _find_analysis_json(sortie_dir)
    if not analysis_path:
        if not quiet:
            print(f"  SKIP  {sortie_dir}  (no analysis JSON)")
        return False

    hires_path = _hires_path_for(analysis_path)
    if os.path.exists(hires_path) and not force:
        if not quiet:
            print(f"  SKIP  {os.path.basename(hires_path)}  (already exists)")
        return False

    with open(analysis_path, encoding="utf-8") as f:
        analysis = json.load(f)

    # Collect ZIPs
    zip_files = sorted(glob.glob(os.path.join(sortie_dir, "*.zip")))
    if exclude_patterns:
        zip_files = [z for z in zip_files
                     if not any(p in os.path.basename(z) for p in exclude_patterns)]
    if not zip_files:
        if not quiet:
            print(f"  SKIP  {sortie_dir}  (no ZIPs)")
        return False

    if not quiet:
        print(f"  {os.path.basename(analysis_path)}  ({len(zip_files)} ZIP(s))")

    t0 = time.perf_counter()

    # Process each ZIP to extract plot_series (keep_plots=True, no trigger)
    n_file_workers = max(1, n_workers // len(zip_files)) if len(zip_files) > 1 else n_workers

    def _process_zip(zip_path):
        tmp = os.path.join(tempfile.gettempdir(),
                           f"_hires_tmp_{os.getpid()}_{hash(zip_path)}.json")
        try:
            ai.process_file(zip_path, tmp, n_workers=n_file_workers,
                            trigger=None, plot_signals=plot_signals,
                            keep_plots=True, quiet=True)
            with open(tmp, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"    ERROR {os.path.basename(zip_path)}: {e}")
            return None
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    with ThreadPool(len(zip_files)) as tp:
        results = [r for r in tp.map(_process_zip, zip_files) if r is not None]

    if not results:
        print(f"  ERROR  {sortie_dir}  (all ZIPs failed)")
        return False

    merged = ai._merge_results(results, trigger=None)
    plot_series = merged.pop("_plot_series", {})
    if not plot_series:
        print(f"  ERROR  {sortie_dir}  (no plot_series extracted)")
        return False

    # Inject mode_transitions from existing analysis for correct windowing
    merged["mode_transitions"] = analysis.get("mode_transitions", [])

    ai._save_hires_file(analysis_path, merged, plot_series)
    elapsed = time.perf_counter() - t0
    if not quiet:
        print(f"    done  ({elapsed:.1f}s)")
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate *_hires.json plot files.")
    parser.add_argument("sortie_dirs", nargs="*",
                        help="Sortie directory name(s) under data_root. Omit to process all.")
    parser.add_argument("--config", default="batch_config.json",
                        help="batch_config.json path (default: batch_config.json)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing hires files")
    parser.add_argument("--workers", type=int, default=None,
                        help="Workers per sortie (default: from config or 8)")
    args = parser.parse_args()

    cfg_path = args.config
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), cfg_path)

    cfg = {}
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            cfg = json.load(f)

    data_root = os.path.abspath(cfg.get("data_root", "."))
    if not os.path.isabs(data_root):
        data_root = os.path.join(os.path.dirname(cfg_path), data_root)

    exclude_patterns = cfg.get("exclude_zip_patterns", [])
    plot_signals_str = cfg.get("plot_signals", "")
    plot_signals = [s.strip() for s in plot_signals_str.split(",") if s.strip()] or None
    n_workers = args.workers or cfg.get("workers", 8)

    # Determine sortie directories to process
    if args.sortie_dirs:
        sortie_dirs = [os.path.join(data_root, d) for d in args.sortie_dirs]
    else:
        # All subdirectories under data_root that contain analysis JSONs
        sortie_dirs = sorted(
            d for d in glob.glob(os.path.join(data_root, "*/"))
            if _find_analysis_json(d)
        )

    if not sortie_dirs:
        print("No sortie directories found.")
        return

    print(f"Generating hires files for {len(sortie_dirs)} sortie(s)  "
          f"[force={args.force}  workers={n_workers}]")

    ok = 0
    for sd in sortie_dirs:
        if process_sortie(sd, exclude_patterns=exclude_patterns,
                          plot_signals=plot_signals,
                          n_workers=n_workers, force=args.force):
            ok += 1

    print(f"\nDone  {ok}/{len(sortie_dirs)} generated")


if __name__ == "__main__":
    main()
