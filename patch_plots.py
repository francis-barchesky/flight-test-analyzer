#!/usr/bin/env python3
"""
patch_plots.py — Add new signals to existing analysis JSONs without re-running full analysis.

ZIPs in a sortie directory are split by signal group — each ZIP contains a
different set of signals covering the full flight time range. This script peeks
at each ZIP's CSV headers to find which ZIP contains the requested signal, opens
only that ZIP, extracts the signal, then patches flight_plots, takeoff_plots, and
each episode's plots into the existing analysis JSON.

Usage:
    python patch_plots.py <signal1,signal2,...> [data_root]
    python patch_plots.py azVoted .
    python patch_plots.py "azVoted,nzVoted" /path/to/data

Options:
    --dry-run    Preview what would be patched without writing
    --force      Re-patch signals even if already present in the JSON
"""

import os, sys, json, zipfile, csv, math, bisect, io, argparse
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from analyze_iads import (
    parse_time_to_s, _signal_matches, _find_time_col, _phase_window,
)
from run_batch import find_sortie_dirs, find_analysis_json

EPISODE_MARGIN_S = 30.0
EPISODE_MAX_PTS  = 500
FLIGHT_MAX_PTS   = 2000   # max points stored in flight_plots / takeoff_plots per signal


def _zip_header_col_map(zip_path, signals):
    """Peek at the CSV header in a ZIP and return {sig: col_index} for matching signals.
    Returns empty dict if none match or on error. Very fast — reads only the first line.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if not csv_names:
                return {}
            with zf.open(csv_names[0]) as raw:
                header_line = raw.readline().decode('utf-8', errors='replace').rstrip('\r\n')
            headers = [h.strip() for h in header_line.split(',')]
            col_map = {}
            for sig in signals:
                for j, h in enumerate(headers):
                    if _signal_matches(h, sig):
                        col_map[sig] = (j, headers)
                        break
            return col_map
    except Exception:
        return {}


def _extract_signals(zip_path, col_map):
    """Extract signals from a ZIP using a pre-built col_map: {sig: (col_idx, headers)}.
    Returns dict: sig → [(t_float, v), ...]
    """
    if not col_map:
        return {}
    result = {sig: [] for sig in col_map}
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if not csv_names:
                return result
            with zf.open(csv_names[0]) as raw:
                reader   = csv.reader(io.TextIOWrapper(raw, encoding='utf-8', errors='replace'))
                headers  = next(reader, None)  # skip header row
                if headers is None:
                    return result
                time_idx = _find_time_col([h.strip() for h in headers])
                for row in reader:
                    if time_idx < 0 or time_idx >= len(row):
                        continue
                    t = parse_time_to_s(row[time_idx])
                    if t is None or not math.isfinite(t):
                        continue
                    for sig, (col_idx, _) in col_map.items():
                        if col_idx < len(row):
                            try:
                                v = float(row[col_idx])
                                if math.isfinite(v):
                                    result[sig].append((t, v))
                            except (ValueError, TypeError):
                                pass
    except Exception as e:
        print(f'    Warning: error reading {os.path.basename(zip_path)}: {e}')
    return result


def patch_sortie(sortie_dir, signals, dry_run=False, force=False):
    json_path = find_analysis_json(sortie_dir)
    if not json_path:
        return False, 'no JSON'

    zip_files = sorted(Path(sortie_dir).glob('*.zip'))
    if not zip_files:
        return False, 'no ZIPs'

    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, f'corrupted JSON: {e}'

    # Determine which signals need patching
    new_sigs = list(signals) if force else [
        s for s in signals if s not in data.get('flight_plots', {})
    ]
    if not new_sigs:
        return False, 'already present (use --force to overwrite)'

    # Find which ZIP contains each signal (peek headers only)
    # sig → (zip_path, col_idx, headers)
    sig_zip = {}
    for zp in zip_files:
        col_map = _zip_header_col_map(str(zp), [s for s in new_sigs if s not in sig_zip])
        for sig, (col_idx, headers) in col_map.items():
            sig_zip[sig] = (str(zp), col_idx, headers)
        if len(sig_zip) == len(new_sigs):
            break  # found all signals — no need to peek further

    missing    = [s for s in new_sigs if s not in sig_zip]
    found_sigs = [s for s in new_sigs if s in sig_zip]
    if not found_sigs:
        return None, f'signal not in ZIPs (not captured for this sortie)'

    if dry_run:
        detail = ', '.join(f'{s} → {os.path.basename(sig_zip[s][0])}' for s in found_sigs)
        return True, f'would patch: {detail}'

    # Group signals by ZIP to minimise open() calls
    zip_to_sigs = {}
    for sig in found_sigs:
        zp, col_idx, headers = sig_zip[sig]
        if zp not in zip_to_sigs:
            zip_to_sigs[zp] = {}
        zip_to_sigs[zp][sig] = (col_idx, headers)

    plot_series = {}
    for zp, col_map in zip_to_sigs.items():
        pts = _extract_signals(zp, col_map)
        for sig, data_pts in pts.items():
            plot_series[sig] = sorted(data_pts, key=lambda p: p[0])

    mode_trans = data.get('mode_transitions', [])

    def _downsample(pts, max_pts):
        if len(pts) <= max_pts:
            return pts
        step = len(pts) / max_pts
        return [pts[int(i * step)] for i in range(max_pts)]

    # ── Patch flight_plots ──
    appr_vals = {'navAppr', 'glidePath', 'align', 'flare', 'retard'}
    fa_lo, fa_hi = _phase_window(mode_trans, appr_vals)
    if 'flight_plots' not in data:
        data['flight_plots'] = {}
    for sig in found_sigs:
        pts = plot_series.get(sig, [])
        if fa_lo is not None:
            pts = [p for p in pts if (fa_lo - 10) <= p[0] <= (fa_hi + 5)]
        pts = _downsample(pts, FLIGHT_MAX_PTS)
        data['flight_plots'][sig] = [[p[0], p[1]] for p in pts]

    # ── Patch takeoff_plots ──
    to_lo, to_hi = _phase_window(mode_trans, {'takeoff', 'takeOff'})
    if to_lo is not None:
        if 'takeoff_plots' not in data:
            data['takeoff_plots'] = {}
        for sig in found_sigs:
            pts = [p for p in plot_series.get(sig, []) if (to_lo - 10) <= p[0] <= (to_hi + 30)]
            pts = _downsample(pts, FLIGHT_MAX_PTS)
            data['takeoff_plots'][sig] = [[p[0], p[1]] for p in pts]

    # ── Patch episode plots ──
    sig_times = {sig: [p[0] for p in plot_series.get(sig, [])] for sig in found_sigs}
    for ep in data.get('episodes', []):
        try:
            ts = float(ep['start_time'])
            te = float(ep.get('end_time') or ts)
        except (TypeError, ValueError):
            continue
        lo, hi = ts - EPISODE_MARGIN_S, te + EPISODE_MARGIN_S
        if 'plots' not in ep:
            ep['plots'] = {}
        for sig in found_sigs:
            times  = sig_times[sig]
            lo_idx = bisect.bisect_left(times,  lo)
            hi_idx = bisect.bisect_right(times, hi)
            sliced = plot_series.get(sig, [])[lo_idx:hi_idx]
            if len(sliced) > EPISODE_MAX_PTS:
                step   = len(sliced) / EPISODE_MAX_PTS
                sliced = [sliced[int(i * step)] for i in range(EPISODE_MAX_PTS)]
            ep['plots'][sig] = [[p[0], p[1]] for p in sliced]

    with open(json_path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))

    detail = ', '.join(f'{s}←{os.path.basename(sig_zip[s][0])}' for s in found_sigs)
    return True, detail


def main():
    parser = argparse.ArgumentParser(
        description='Patch new plot signals into existing analysis JSONs.'
    )
    parser.add_argument('signals',   nargs='?', default='', help='Comma-separated signal names')
    parser.add_argument('data_root', nargs='?', default='.', help='Data root (default: .)')
    parser.add_argument('--dry-run',         action='store_true')
    parser.add_argument('--force',           action='store_true', help='Overwrite existing signal data')
    parser.add_argument('--list-signals',    action='store_true', help='List all available signals in the first sortie and exit')
    parser.add_argument('--workers',         type=int, default=4, help='Parallel workers (default: 4)')
    parser.add_argument('--fix-corrupted',   action='store_true', help='Find (and delete) corrupted analysis JSONs so the batch re-analyzes them')
    args = parser.parse_args()

    signals   = [s.strip() for s in args.signals.split(',') if s.strip()]
    data_root = os.path.abspath(args.data_root)

    print(f'Signals   : {signals}')
    print(f'Data root : {data_root}')
    if args.dry_run: print('DRY RUN')
    if args.force:   print('--force (overwriting existing)')
    print()

    sortie_dirs = find_sortie_dirs(data_root)
    if not sortie_dirs:
        print('No sortie directories found.')
        sys.exit(1)

    # --fix-corrupted: scan all JSONs, report bad ones, delete if not dry-run
    if args.fix_corrupted:
        print(f'Scanning {len(sortie_dirs)} sortie(s) for corrupted JSONs...\n')
        bad = []
        for sd in sortie_dirs:
            jp = find_analysis_json(sd)
            if not jp:
                continue
            try:
                with open(jp, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError as e:
                bad.append((jp, str(e)))
                action = '[dry-run]' if args.dry_run else 'DELETED'
                print(f'  ✗  {os.path.basename(sd):<35}  {action}  {e}')
                if not args.dry_run:
                    os.remove(jp)
        if not bad:
            print('  No corrupted JSONs found.')
        else:
            print(f'\n{len(bad)} corrupted JSON(s) {"found (not deleted — dry-run)" if args.dry_run else "deleted"}.')
            if not args.dry_run:
                print('Run  python run_batch.py batch_config.json  to re-analyze.')
        sys.exit(0)

    # --list-signals: dump all column names from the first sortie and exit
    if args.list_signals:
        first_zips = sorted(Path(sortie_dirs[0]).glob('*.zip'))
        print(f'Available signals in {os.path.basename(sortie_dirs[0])}:\n')
        for zp in first_zips:
            try:
                with zipfile.ZipFile(str(zp), 'r') as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                    if not csv_names:
                        continue
                    with zf.open(csv_names[0]) as raw:
                        header_line = raw.readline().decode('utf-8', errors='replace')
                    headers = [h.strip() for h in header_line.split(',')]
                    suffixes = sorted({h.split('.')[-1] for h in headers if h})
                    print(f'  {os.path.basename(str(zp))}  ({len(headers)} cols):')
                    for s in suffixes:
                        print(f'    {s}')
            except Exception as e:
                print(f'  {os.path.basename(str(zp))}: error — {e}')
        sys.exit(0)

    # Early validation: peek the first sortie's ZIPs to confirm signals exist.
    # If none of the requested signals are found, abort and show available signals.
    first_zips = sorted(Path(sortie_dirs[0]).glob('*.zip'))
    all_headers = set()
    confirmed = set()
    for zp in first_zips:
        try:
            with zipfile.ZipFile(str(zp), 'r') as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                if not csv_names:
                    continue
                with zf.open(csv_names[0]) as raw:
                    header_line = raw.readline().decode('utf-8', errors='replace')
                headers = [h.strip() for h in header_line.split(',')]
                all_headers.update(headers)
                for sig in signals:
                    if any(_signal_matches(h, sig) for h in headers):
                        confirmed.add(sig)
        except Exception:
            pass

    not_found = [s for s in signals if s not in confirmed]
    if not_found:
        print(f'ERROR: signal(s) not found in {os.path.basename(sortie_dirs[0])}: {not_found}')
        # Suggest close matches
        for sig in not_found:
            sig_l = sig.lower()
            matches = sorted({h for h in all_headers if sig_l in h.lower() or h.lower().endswith('.' + sig_l)})
            if matches:
                print(f'  Did you mean one of: {matches[:10]}')
            else:
                print(f'  No close matches found. Use --list-signals to see all available signals.')
        print()
        if not confirmed:
            sys.exit(1)
        print(f'Continuing with found signals only: {sorted(confirmed)}\n')
        signals = [s for s in signals if s in confirmed]

    import time, threading, datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fmt(secs):
        secs = int(secs)
        h, r = divmod(secs, 3600)
        m, s = divmod(r, 60)
        if h:   return f'{h}h {m:02d}m {s:02d}s'
        if m:   return f'{m}m {s:02d}s'
        return f'{s}s'

    n          = len(sortie_dirs)
    pad        = len(str(n))
    wall_start = time.perf_counter()
    print_lock = threading.Lock()
    done_count = 0
    ok = skip = fail = 0

    def _run(sd):
        t0 = time.perf_counter()
        try:
            patched, msg = patch_sortie(sd, signals, dry_run=args.dry_run, force=args.force)
        except Exception as e:
            patched, msg = False, f'error: {e}'
        return sd, patched, msg, time.perf_counter() - t0

    workers = min(args.workers, n)
    pool = ThreadPoolExecutor(max_workers=workers)
    futures = {pool.submit(_run, sd): sd for sd in sortie_dirs}
    try:
        for fut in as_completed(futures):
            sd, patched, msg, elapsed = fut.result()
            name = os.path.basename(sd)
            icon = '✓' if patched is True else ('~' if patched is None else ('–' if 'already' in msg or 'no JSON' in msg else '✗'))
            with print_lock:
                done_count += 1
                wall = time.perf_counter() - wall_start
                rate = done_count / wall
                eta_s   = (n - done_count) / rate if rate > 0 else 0
                eta_dt  = datetime.datetime.now() + datetime.timedelta(seconds=eta_s)
                eta_str = eta_dt.strftime('%H:%M:%S')
                print(f'  [{done_count:>{pad}}/{n}]  {icon}  {name:<35}  {msg:<40}  {elapsed:.1f}s  ETA {eta_str}')
                if patched is True:                          ok   += 1
                elif patched is None:                        skip += 1
                elif 'already' in msg or 'no JSON' in msg:  skip += 1
                else:                                        fail += 1
    except KeyboardInterrupt:
        print('\n  Interrupted — cancelling pending work...')
        pool.shutdown(wait=False, cancel_futures=True)
        total = time.perf_counter() - wall_start
        print(f'  Stopped after {done_count}/{n} sorties  ({_fmt(total)} elapsed)')
        print(f'  {ok} patched, {skip} skipped, {fail} failed so far')
        sys.exit(1)
    else:
        pool.shutdown(wait=False)

    total = time.perf_counter() - wall_start
    print(f'\nDone — {ok} patched, {skip} skipped, {fail} failed  ({_fmt(total)} total)')


if __name__ == '__main__':
    main()
