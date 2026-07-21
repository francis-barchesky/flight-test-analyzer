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
import gzip
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta


def _jira_sw_version(sortie_name):
    """Query FFT Jira for the SW version embedded in the matching Flight Test issue summary.

    Reads credentials from ~/.cia_config.json (same file as CIA generator).
    Returns a version string like '5.01.04', or None if not found / auth missing.
    """
    try:
        cfg_path = pathlib.Path.home() / '.cia_config.json'
        if not cfg_path.exists():
            return None
        cfg = json.loads(cfg_path.read_text())
        token = cfg.get('jiraToken', '')
        auth_type = cfg.get('jiraAuthType', 'Basic')
        base_url = cfg.get('jiraBaseUrl', 'https://merlinlabs.atlassian.net').rstrip('/')
        if not token:
            return None

        search_tag = sortie_name.replace('_', '')  # S140_N208B -> S140N208B
        jql = (f'project = FFT AND issuetype = "Flight Test" '
               f'AND summary ~ "{search_tag}" ORDER BY updated DESC')
        url = (f'{base_url}/rest/api/3/issue/search'
               f'?jql={urllib.parse.quote(jql)}&fields=summary&maxResults=1')

        req = urllib.request.Request(url, headers={
            'Authorization': f'{auth_type} {token}',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        issues = data.get('issues', [])
        if not issues:
            return None
        summary = issues[0].get('fields', {}).get('summary', '')
        m = re.search(r'\b(\d+\.\d+\.\d+)\b', summary)
        return m.group(1) if m else None
    except Exception:
        return None


_FLIGHT_CARDS_FOLDER = '10E-pP0fwACMaOvoeYGLaePEyl4kZH9zV'


def _gdrive_sw_version(sortie_name, folder_id=_FLIGHT_CARDS_FOLDER):
    """Fallback: search the Flight Cards Google Drive folder for SW version.

    Looks for a Google Doc whose title contains the sortie tag, exports it as
    plain text, and extracts the FCC/FTS version line (e.g. 'FCC - 05.01.04').
    Uses googleToken from ~/.cia_config.json (refresh with setupCiaAuth.m if expired).
    """
    try:
        cfg_path = pathlib.Path.home() / '.cia_config.json'
        if not cfg_path.exists():
            return None
        cfg = json.loads(cfg_path.read_text())
        token = cfg.get('googleToken', '')
        if not token:
            return None

        search_tag = sortie_name.replace('_', '')  # S140_N208B -> S140N208B
        query = f"'{folder_id}' in parents and title contains '{search_tag}'"
        search_url = (
            'https://www.googleapis.com/drive/v3/files'
            f'?q={urllib.parse.quote(query)}'
            '&fields=files(id,name)'
            '&includeItemsFromAllDrives=true'
            '&supportsAllDrives=true'
            '&pageSize=1'
        )
        req = urllib.request.Request(search_url, headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        files = data.get('files', [])
        if not files:
            return None

        file_id = files[0]['id']
        export_url = (
            f'https://www.googleapis.com/drive/v3/files/{file_id}/export'
            '?mimeType=text/plain'
        )
        req = urllib.request.Request(export_url, headers={
            'Authorization': f'Bearer {token}',
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode('utf-8', errors='replace')

        # Match "FCC - 05.01.04" or "FTS - 05.01.04" (zero-padded format)
        m = re.search(r'\b(?:FCC|FTS)\s*[-–]\s*(\d{2}\.\d{2}\.\d{2})\b', content)
        return m.group(1) if m else None
    except Exception:
        return None


def _patch_json(path, extra):
    """Merge extra fields into an existing analysis JSON (plain or gzip)."""
    if path.endswith('.gz'):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        data.update(extra)
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(data, f)
    else:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data.update(extra)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f)


def _flight_date_iso(existing_json):
    """
    Best-effort flight date from an analysis JSON (plain or gzip-compressed).

    The JSON has `rec_start_s` (seconds since start-of-year of the flight year)
    and `generated_at` (analysis timestamp). The flight year is the same as
    or one earlier than the analysis year — pick whichever makes the computed
    flight date <= analysis date. Returns 'YYYY-MM-DD' or None.
    """
    try:
        import gzip as _gzip
        if existing_json.endswith('.json.gz'):
            with _gzip.open(existing_json, 'rt', encoding='utf-8') as f:
                d = json.load(f)
        else:
            with open(existing_json, encoding='utf-8') as f:
                d = json.load(f)
        rec = d.get('rec_start_s')
        gen = d.get('generated_at')
        if rec is None or not gen:
            return None
        gen_date = datetime.fromisoformat(gen[:10])
        # IADS day-of-year is 0-indexed in the Time column (Day 000 = Jan 1).
        doy0 = int(float(rec) / 86400)
        # doy0 == 0 means the time column was HH:MM:SS (seconds-since-midnight),
        # not DDD:HH:MM:SS day-of-year — flight date is indeterminate.
        if doy0 == 0:
            return None
        candidate = datetime(gen_date.year, 1, 1) + timedelta(days=doy0)
        if candidate.date() > gen_date.date():
            candidate = datetime(gen_date.year - 1, 1, 1) + timedelta(days=doy0)
        return candidate.date().isoformat()
    except Exception:
        return None


# ── Sortie name from filename ──────────────────────────────────────────────────

def sortie_from_filename(filename):
    """
    Extract a sortie tag + tail number from a ZIP filename.
    e.g.  AFCS_del3_v20260202_S107N208B_2.zip  ->  S107_2_N208B
          AFCS_del3_v20260202_G011ZKMLN.zip     ->  G011_ZKMLN
    Returns None if no sortie tag can be found.
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    # With leg number: S107N208B_2 -> S107_2_N208B
    m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})([A-Z][A-Z0-9]+)_(\d{1,3})(?!\d)', name, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}_{m.group(3)}_{m.group(2).upper()}"
    # Without leg number: G011ZKMLN -> G011_ZKMLN
    m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})([A-Z][A-Z0-9]+)', name, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}_{m.group(2).upper()}"
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
    one ZIP file OR an existing analysis JSON, sorted by directory name.
    """
    dirs = []
    def _dir_sort_key(entry_name):
        m = re.match(r'^([SG])(\d+)(?:_(\d+))?', entry_name, re.IGNORECASE)
        if m:
            return (m.group(1).upper(), int(m.group(2)), int(m.group(3) or 0))
        return ('~', 0, 0)

    try:
        entries = sorted(os.scandir(data_root), key=lambda e: _dir_sort_key(e.name))
    except FileNotFoundError:
        print(f"ERROR: data_root not found: {data_root}")
        sys.exit(1)

    for entry in entries:
        if not entry.is_dir():
            continue
        has_zips = bool(glob.glob(os.path.join(entry.path, "*.zip")))
        has_json = bool(glob.glob(os.path.join(entry.path, "analysis*.json"))) or \
                   bool(glob.glob(os.path.join(entry.path, "analysis*.json.gz")))
        if has_zips or has_json:
            dirs.append(entry.path)
    return dirs


def find_analysis_json(sortie_dir):
    """
    Return the path of an existing analysis JSON (plain or gzip) in sortie_dir, or None.
    Prefers .json.gz over .json when both exist. Excludes legacy *_hires.json companions.
    """
    plain = [
        p for p in glob.glob(os.path.join(sortie_dir, "analysis*.json"))
        if not p.lower().endswith("_hires.json")
    ]
    compressed = glob.glob(os.path.join(sortie_dir, "analysis*.json.gz"))
    candidates = plain + compressed
    if candidates:
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
    ap.add_argument("--zips-only", action="store_true",
                    help="Iterate only sortie dirs that contain at least one ZIP "
                         "(skip dirs that only have an old analysis JSON). Useful "
                         "from pipeline.sh so each day's pass touches only freshly-"
                         "downloaded sorties.")
    ap.add_argument("--from-date", default=None,
                    help="Re-analyze only sorties whose flight date is >= YYYY-MM-DD. "
                         "Overrides skip_existing for matching sorties; pre-date sorties "
                         "are skipped regardless of skip_existing. Requires ZIPs to be "
                         "present (sorties without ZIPs are always skipped).")
    args = ap.parse_args()

    config_path = os.path.abspath(args.config)
    cfg = load_config(config_path)
    config_dir = os.path.dirname(config_path)

    # Resolve script path relative to run_batch.py's own directory, not the config.
    # This allows chunk configs in subdirectories to still find analyze_iads.py.
    script = cfg.get("script", "analyze_iads.py")
    if not os.path.isabs(script):
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), script)
    if not os.path.exists(script):
        print(f"ERROR: analyze_iads.py not found at: {script}")
        sys.exit(1)

    # Resolve data_root relative to run_batch.py's own directory so that chunk
    # configs in subdirectories (e.g. pipeline_chunks/) still find the right root.
    data_root = cfg.get("data_root", ".")
    if not os.path.isabs(data_root):
        data_root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), data_root))

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
                    import gzip as _gzip
                    if j.endswith('.json.gz'):
                        with _gzip.open(j, 'rt', encoding='utf-8') as f:
                            d = json.load(f)
                    else:
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

    output_dir       = cfg.get("output_dir")
    trigger          = cfg.get("trigger", "afcsCapable")
    trigger_from     = cfg.get("trigger_from", 1.0)
    trigger_to       = cfg.get("trigger_to", 0.0)
    workers          = cfg.get("workers", 0)
    plot_signals     = cfg.get("plot_signals", "radAltVoted,gndSpdVoted")
    skip_existing    = cfg.get("skip_existing", True)
    exclude_zips     = cfg.get("exclude_zip_patterns", [])
    delete_after     = cfg.get("delete_zips_after", True)
    parallel_sorties = args.parallel_sorties or cfg.get("parallel_sorties", 1)
    trace_graph_map  = cfg.get("trace_graph_map", {})
    detected_cores   = os.cpu_count() or 1
    workers          = workers or detected_cores

    # Resolve download_start_date (used by the no-zips guard to flag pre-window
    # sorties). Accepts ISO YYYY-MM-DD or the literal "today".
    _start_raw = cfg.get("download_start_date")
    if _start_raw and str(_start_raw).lower() == "today":
        download_start_date = datetime.now().date().isoformat()
    else:
        download_start_date = _start_raw

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
    if args.zips_only:
        before = len(sortie_dirs)
        sortie_dirs = [d for d in sortie_dirs
                       if glob.glob(os.path.join(d, "*.zip"))]
        if before != len(sortie_dirs):
            print(f"  --zips-only: {before - len(sortie_dirs)} dir(s) without ZIPs skipped")

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
        out_path = os.path.join(out_dir, "analysis.json.gz")

        # ── Skip check ────────────────────────────────────────────────────────
        # --from-date: determine whether this sortie is in-window.
        # In-window sorties ignore skip_existing (always re-run if ZIPs present).
        # Pre-window sorties are always skipped, even if skip_existing is False.
        from_date = args.from_date
        if from_date and existing_json:
            fdate = _flight_date_iso(existing_json)
            if fdate is not None and fdate < from_date:
                print(f"[{i}/{n}]  {name}  SKIP  pre-date ({fdate})", flush=True)
                return {"sortie": name, "json": existing_json, "status": "skipped"}
            # In-window (or indeterminate date): fall through to re-analyze (ignore skip_existing)
        elif skip_existing and existing_json:
            print(f"[{i}/{n}]  {name}  SKIP  ({os.path.basename(existing_json)})", flush=True)
            if delete_after and zips:
                delete_zips(sortie_dir, dry_run=args.dry_run)
            return {"sortie": name, "json": existing_json, "status": "skipped"}

        # ── No-ZIP guard ──────────────────────────────────────────────────────
        # When skip_existing=False, the batch wants to re-analyze every sortie.
        # But dirs with only an old analysis JSON (ZIPs deleted in a prior run)
        # have nothing to re-analyze — calling analyze_iads.py would just error
        # out with "No ZIP files found". Keep the existing JSON in place.
        if not zips:
            fdate = _flight_date_iso(existing_json) if existing_json else None
            pre_window = bool(fdate and download_start_date and fdate < download_start_date)
            if pre_window:
                date_tag = f"  pre-window (flight {fdate})"
            elif fdate:
                date_tag = f"  (flight {fdate})"
            else:
                date_tag = ""
            jtag = f"  ({os.path.basename(existing_json)})" if existing_json else ""
            print(f"[{i}/{n}]  {name}  NO-ZIPS{date_tag}{jtag}", flush=True)
            return {"sortie": name, "json": existing_json, "status": "no-zips",
                    "flight_date": fdate, "pre_window": pre_window}

        # ── Dry run ───────────────────────────────────────────────────────────
        # Match sortie name against trace_graph_map.
        # Keys with '_' (e.g. "S125_N208B") match on sortie prefix + tail so
        # "S125_N208B" hits S125_N208B, S125_2_N208B, S125_3_N208B, etc.
        # Keys without '_' fall back to plain substring match.
        def _tg_match(key, sname):
            if "_" in key:
                kparts = key.split("_", 1)
                sparts = sname.split("_")
                return sparts[0] == kparts[0] and sparts[-1] == kparts[1]
            return key in sname
        tg_version = next((v for k, v in trace_graph_map.items() if _tg_match(k, name)), None)
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
        if exclude_zips:
            cmd += ["--exclude-zips", ",".join(exclude_zips)]
        if tg_version:
            cmd += ["--trace-graph", tg_version]
        if args.dry_run:
            print(f"[{i}/{n}]  {name}  [dry-run]", flush=True)
            return {"sortie": name, "json": None, "status": "dry-run"}

        # ── SW version lookup (quick Jira/GDrive call before analysis) ──────────
        jira_ver = _jira_sw_version(name) or _gdrive_sw_version(name)
        sw_tag = f"  sw={jira_ver}" if jira_ver else ""

        # ── Run ───────────────────────────────────────────────────────────────
        print(f"[{i}/{n}]  {name}{sw_tag}  ({len(zips)} ZIP(s))", flush=True)
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

            if jira_ver and written_json:
                _patch_json(written_json, {'jira_sw_version': jira_ver})

            return {"sortie": name, "json": written_json, "status": "ok", "elapsed_s": round(elapsed, 1), "sw_version": jira_ver}

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
        done    = sum(1 for r in results if r["status"] in ("ok", "skipped", "no-zips"))
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
    no_zips = sum(1 for r in results if r["status"] == "no-zips")
    errors  = sum(1 for r in results if r["status"] == "error")

    manifest_tag = f"  manifest={manifest_path}" if not args.dry_run else ""
    nz_tag       = f"  no-zips={no_zips}" if no_zips else ""
    print(f"Batch done  {total_elapsed:.1f}s  |  ok={ok}  skipped={skipped}{nz_tag}  errors={errors}{manifest_tag}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
