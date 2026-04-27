#!/usr/bin/env python3
"""
patch_signal_from_zip.py — Extract a specific signal from a ZIP/CSV and patch
it into the matching analysis_*.json.

Useful when a signal is missing from existing exports and a targeted re-export
is done in IADS for just that signal.

Usage:
    python patch_signal_from_zip.py <zip_or_csv> [--signal sysNotEngage] [--data-root .]
    python patch_signal_from_zip.py myexport.zip
    python patch_signal_from_zip.py myexport.zip --signal sysNotEngage --data-root ./sorties

The script:
  1. Reads the ZIP (or raw CSV) and finds the target signal column.
  2. Extracts all bool/enum transitions for that signal.
  3. Scans analysis_*.json files under data_root for the one whose time window
     overlaps the CSV's time range.
  4. Patches the signal's transitions into result["<signalName>"] (top-level key).
  5. Also injects the signal name into result["bool_channels"] if not present.
"""

import csv
import io
import json
import math
import os
import sys
import zipfile
import re
import argparse


_ENUM_MAX = 32   # consistent with analyze_iads.py


def _read_csv_transitions(fileobj, signal_re):
    """
    Stream a CSV file-like object and return (transitions, t_min, t_max, found_signals).
    signal_re: compiled regex matching signal column names.
    """
    reader = csv.reader(io.TextIOWrapper(fileobj, encoding='utf-8', errors='replace'))
    headers = None
    time_col = -1
    target_cols = []   # list of (col_index, header_name)

    transitions = []
    t_min = math.inf
    t_max = -math.inf

    col_prev = {}
    col_has_non_bool = {}
    col_is_integer = {}
    col_unique = {}

    for row in reader:
        if headers is None:
            headers = row
            for i, h in enumerate(headers):
                h = h.strip()
                if re.search(r'^(time|irig|t)\b', h, re.I) and time_col < 0:
                    time_col = i
                if signal_re.search(h):
                    target_cols.append((i, h))
                    col_has_non_bool[i] = False
                    col_is_integer[i]   = True
                    col_unique[i]       = set()
            continue

        if time_col < 0 or not row:
            continue

        try:
            t = float(row[time_col])
        except (ValueError, IndexError):
            continue

        if math.isfinite(t):
            if t < t_min: t_min = t
            if t > t_max: t_max = t

        for (j, name) in target_cols:
            try:
                v = float(row[j])
            except (ValueError, IndexError):
                continue
            if not math.isfinite(v):
                continue

            if v != 0.0 and v != 1.0:
                col_has_non_bool[j] = True
            if col_is_integer[j]:
                if v % 1.0 != 0.0:
                    col_is_integer[j] = False
                elif len(col_unique[j]) <= _ENUM_MAX:
                    col_unique[j].add(v)

            prev = col_prev.get(j)
            if prev is not None and prev != v:
                is_bool = not col_has_non_bool[j]
                is_enum = col_is_integer[j] and 0 < len(col_unique[j]) <= _ENUM_MAX
                if is_bool or is_enum:
                    transitions.append({
                        'signal': name,
                        'time':   t,
                        'from':   prev,
                        'to':     v,
                    })
            col_prev[j] = v

    found = [name for (_, name) in target_cols]
    return transitions, t_min, t_max, found


def _load_transitions_from_source(source_path, signal_re):
    """Load transitions from a ZIP or CSV file."""
    transitions = []
    t_min, t_max = math.inf, -math.inf
    found_signals = []

    if source_path.lower().endswith('.zip'):
        with zipfile.ZipFile(source_path) as zf:
            for name in zf.namelist():
                if not name.lower().endswith('.csv'):
                    continue
                with zf.open(name) as f:
                    tr, lo, hi, found = _read_csv_transitions(f, signal_re)
                    transitions.extend(tr)
                    if lo < t_min: t_min = lo
                    if hi > t_max: t_max = hi
                    found_signals.extend(found)
    else:
        with open(source_path, 'rb') as f:
            transitions, t_min, t_max, found_signals = _read_csv_transitions(f, signal_re)

    transitions.sort(key=lambda t: float(t['time']))
    return transitions, t_min, t_max, list(set(found_signals))


def _find_matching_json(data_root, t_min, t_max):
    """Find analysis_*.json whose rec_start_s / duration_s overlaps [t_min, t_max]."""
    candidates = []
    for root, dirs, files in os.walk(data_root):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for fn in files:
            if fn.startswith('analysis_') and fn.endswith('.json') and '_hires' not in fn:
                candidates.append(os.path.join(root, fn))

    best = None
    best_overlap = -1

    for path in candidates:
        try:
            with open(path, encoding='utf-8') as f:
                d = json.load(f)
        except Exception:
            continue
        r0 = d.get('rec_start_s')
        dur = d.get('duration_s')
        if r0 is None or dur is None:
            continue
        r1 = r0 + dur
        overlap = min(r1, t_max) - max(r0, t_min)
        if overlap > best_overlap:
            best_overlap = overlap
            best = (path, d, overlap)

    return best


def patch(source_path, signal_name, data_root='.', force=False):
    signal_re = re.compile(re.escape(signal_name), re.I)

    print(f"Reading {os.path.basename(source_path)} …")
    transitions, t_min, t_max, found = _load_transitions_from_source(source_path, signal_re)

    if not found:
        print(f"  ERROR: signal matching '{signal_name}' not found in source file.")
        print(f"         Check the column names in the CSV.")
        return False

    print(f"  Found signals: {found}")
    print(f"  Time range: {t_min:.1f} – {t_max:.1f}s  ({len(transitions)} transitions)")

    match = _find_matching_json(data_root, t_min, t_max)
    if not match:
        print(f"  ERROR: no analysis_*.json in '{data_root}' overlaps that time range.")
        return False

    json_path, data, overlap = match
    print(f"  Matched: {os.path.relpath(json_path, data_root)}  (overlap {overlap:.1f}s)")

    # Use the canonical signal name key (bare suffix without lane prefix)
    key = signal_name.split('.')[-1]

    if data.get(key) and not force:
        print(f"  Skipped: '{key}' already present (use --force to overwrite).")
        return False

    data[key] = transitions

    # Ensure signal names appear in bool_channels
    bools = data.get('bool_channels', [])
    for sig in found:
        if sig not in bools:
            bools.append(sig)
    data['bool_channels'] = bools

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, separators=(',', ':'), allow_nan=False)

    rises = sum(1 for t in transitions if t.get('to') == 1)
    print(f"  Patched '{key}': {len(transitions)} transition(s), {rises} assertion(s)  →  {os.path.relpath(json_path, data_root)}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Patch a signal from a ZIP/CSV into an analysis JSON.')
    parser.add_argument('source', help='ZIP or CSV file containing the target signal')
    parser.add_argument('--signal', default='sysNotEngage', help='Signal name to extract (default: sysNotEngage)')
    parser.add_argument('--data-root', default='.', help='Root directory to search for analysis_*.json (default: .)')
    parser.add_argument('--force', action='store_true', help='Overwrite existing key if present')
    args = parser.parse_args()

    ok = patch(args.source, args.signal, data_root=args.data_root, force=args.force)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
