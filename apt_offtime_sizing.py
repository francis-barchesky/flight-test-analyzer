#!/usr/bin/env python3
"""
apt_offtime_sizing.py  --  Size AptTrimBiasReqTimeOff from flight data.

Purpose
-------
Quantify the worst-case "above-threshold dwell" of radAltVoted during the
approach window so the new debounce OFF time (aptBiasReqDeb.offTime, fed by a
proposed PDI param pdiAptTrimBiasReqTimeOff) can be chosen with margin instead
of eyeballed off a plot.

Root-cause model (confirmed on S041_2_ZKMLN):
  radAltStart = radAltVoted < AptTrimBiasAltThresh   -> debounce -> request -> LATCH
With offTime = 0, any single-step drop-out of radAltStart (radAltVoted
momentarily >= threshold while dithering across it) collapses the request and
produces the in/out bias. offTime must exceed the longest such drop-out dwell
we want to ride through, while staying shorter than a genuine climb-out.

Supported inputs
----------------
* analyze_iads.py JSON  (e.g. S041_2_ZKMLN/analysis_S041_2_hires.json)
* .csv / .parquet wide frames

JSON layout is auto-detected. Handles:
  {"signals": {"radAltVoted": {"time": [...], "data": [...]}}}
  {"radAltVoted": {"t": [...], "y": [...]}}
  {"time": [...], "radAltVoted": [...]}                       (flat parallel)
  {"signals": {"radAltVoted": [...]}}  + a shared time vector elsewhere
  [{"time": .., "radAltVoted": ..}, ...]                      (records)

Usage
-----
  python apt_offtime_sizing.py --data <file_or_dir> [--sortie S041_2_ZKMLN]
         [--threshold 500] [--margin 1.0] [--on-time 1.0]
         [--signal-col radAltVoted] [--time-col time] [--time-scale 1.0]

  python apt_offtime_sizing.py --data <file> --list-signals   # discover names

Notes
-----
* --threshold must be the real pdiAptTrimBiasAltThresh (default 500 ft from DR).
* Prefer the *_hires.json for accurate dwell timing.
* If time is in ms, pass --time-scale 0.001.
"""

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np

try:
    import pandas as pd
except ImportError:
    pd = None  # only needed for csv/parquet

VALUE_KEYS = ["data", "values", "value", "y", "samples", "series", "d"]
TIME_KEYS = ["time", "t", "x", "irig", "irigtime", "irig_time", "irigTime",
             "timestamp", "ts", "sec", "seconds"]


# ----------------------------------------------------------------------------- JSON
def _is_num_list(x):
    return isinstance(x, list) and len(x) > 0 and all(
        isinstance(v, (int, float)) and not isinstance(v, bool) for v in x[:64]
    )


def _parse_time_token(v):
    """Numeric, numeric-string, or IRIG 'DDD:HH:MM:SS.mmm' / 'HH:MM:SS.mmm' -> seconds."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip()
    if ":" in s:
        parts = [float(x) for x in s.split(":")]
        mult = [1.0, 60.0, 3600.0, 86400.0]
        return sum(p * mult[j] for j, p in enumerate(reversed(parts)))
    return float(s)


def _to_seconds_array(a):
    arr = np.asarray(a)
    if arr.dtype.kind in "fiu":
        return arr.astype(float)
    return np.asarray([_parse_time_token(v) for v in a], dtype=float)


def _looks_like_time_token(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return True
    if isinstance(v, str):
        t = v.strip()
        if re.fullmatch(r"\d+(:\d+){1,3}(\.\d+)?", t):
            return True
        try:
            float(t); return True
        except ValueError:
            return False
    return False


def _is_time_list(x):
    return isinstance(x, list) and len(x) > 0 and all(_looks_like_time_token(v) for v in x[:64])


def _walk(obj, path=""):
    """Yield (path, key, value) for every dict entry, depth-first."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            yield p, k, v
            yield from _walk(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]")


def _find_signal_node(obj, name):
    """Find the value associated with a key matching `name` (exact, then contains)."""
    name_l = name.lower()
    exact, partial = None, None
    for _p, k, v in _walk(obj):
        if not isinstance(k, str):
            continue
        if k.lower() == name_l and exact is None:
            exact = v
        elif name_l in k.lower() and partial is None:
            partial = v
    return exact if exact is not None else partial


def _arrays_from_node(node):
    """Return (time, values) from a signal node, or (None, values), or (None, None)."""
    # node is a numeric list -> values only
    if _is_num_list(node):
        return None, np.asarray(node, dtype=float)
    # node is list of records or [t, v] pairs
    if isinstance(node, list) and node and isinstance(node[0], dict):
        tk = next((k for k in node[0] if k.lower() in TIME_KEYS), None)
        vk = next((k for k in node[0] if k.lower() in VALUE_KEYS or k.lower() == "v"), None)
        if tk and vk:
            t = _to_seconds_array([r[tk] for r in node])
            y = np.asarray([r[vk] for r in node], dtype=float)
            return t, y
    if isinstance(node, list) and node and isinstance(node[0], (list, tuple)) and len(node[0]) == 2:
        arr = np.asarray(node, dtype=float)
        return arr[:, 0], arr[:, 1]
    # node is a dict -> pull value array and (optional) time array from it
    if isinstance(node, dict):
        vk = next((k for k in node if k.lower() in VALUE_KEYS and _is_num_list(node[k])), None)
        tk = next((k for k in node if k.lower() in TIME_KEYS and _is_time_list(node[k])), None)
        vals = np.asarray(node[vk], dtype=float) if vk else None
        tim = _to_seconds_array(node[tk]) if tk else None
        if vals is None:  # maybe the only numeric list present is the data
            nums = [k for k in node if _is_num_list(node[k])]
            if len(nums) == 1:
                vals = np.asarray(node[nums[0]], dtype=float)
        return tim, vals
    return None, None


def _find_global_time(obj, n):
    """Find any time-named numeric array of length n anywhere in the object."""
    best = None
    for _p, k, v in _walk(obj):
        if isinstance(k, str) and k.lower() in TIME_KEYS and _is_time_list(v) and len(v) == n:
            best = _to_seconds_array(v)
            break
    return best



def _match_key(d, name):
    nl = name.lower()
    for k in d:
        if isinstance(k, str) and k.lower() == nl:
            return k
    for k in d:
        if isinstance(k, str) and nl in k.lower():
            return k
    return None


def _find_record_list(obj, signal_col):
    """Find a list-of-dicts whose elements carry the signal key (record-style JSON)."""
    if isinstance(obj, list) and obj and isinstance(obj[0], dict) and _match_key(obj[0], signal_col):
        return obj
    for _p, _k, v in _walk(obj):
        if isinstance(v, list) and v and isinstance(v[0], dict) and _match_key(v[0], signal_col):
            return v
    return None

def load_json_series(path, signal_col, time_col):
    obj = json.loads(Path(path).read_text())
    rec = _find_record_list(obj, signal_col)
    if rec is not None:
        vk = _match_key(rec[0], signal_col)
        tk = _match_key(rec[0], time_col) or next(
            (k for k in rec[0] if isinstance(k, str) and k.lower() in TIME_KEYS), None)
        y = np.asarray([r[vk] for r in rec], dtype=float)
        if tk is None:
            sys.exit("Record-style JSON found but no time key in records; pass --time-col.")
        t = _to_seconds_array([r[tk] for r in rec])
        n = min(len(t), len(y))
        return t[:n], y[:n]
    node = _find_signal_node(obj, signal_col)
    if node is None:
        names = sorted({k for _p, k, v in _walk(obj)
                        if isinstance(k, str) and (_is_num_list(v) or isinstance(v, dict))})
        sys.exit(f"Signal '{signal_col}' not found. Candidate keys:\n  " + ", ".join(names[:60]))
    t, y = _arrays_from_node(node)
    if y is None:
        sys.exit(f"Found '{signal_col}' but couldn't extract a numeric value array from it.")
    if t is None:  # need a time vector from elsewhere
        # try explicit --time-col first, then any global time array of matching length
        tnode = _find_signal_node(obj, time_col)
        if tnode is not None:
            _t2, t = _arrays_from_node(tnode if isinstance(tnode, (dict, list)) else {"data": tnode})
        if t is None:
            t = _find_global_time(obj, len(y))
    if t is None:
        sys.exit("Could not locate a time vector. Pass --time-col with the correct key name.")
    n = min(len(t), len(y))
    return t[:n], y[:n]


# ----------------------------------------------------------------------------- tabular
def _columns_of(path):
    try:
        if path.suffix.lower() in (".parquet", ".pq"):
            import pyarrow.parquet as pq
            return list(pq.ParquetFile(path).schema.names)
        if path.suffix.lower() == ".json":
            obj = json.loads(path.read_text())
            return sorted({k for _p, k, v in _walk(obj) if isinstance(k, str)})
        return list(pd.read_csv(path, nrows=0).columns)
    except Exception:
        return []


def load_tabular_series(path, signal_col, time_col):
    if pd is None:
        sys.exit("pandas required for csv/parquet:  pip install pandas pyarrow")
    df = pd.read_parquet(path) if path.suffix.lower() in (".parquet", ".pq") else pd.read_csv(path)
    for col in (time_col, signal_col):
        if col not in df.columns:
            sys.exit(f"Column '{col}' not in {path.name}. Columns: {list(df.columns)}")
    d = df[[time_col, signal_col]].dropna()
    return d[time_col].to_numpy(float), d[signal_col].to_numpy(float)


# ----------------------------------------------------------------------------- discovery
def find_data_file(data, sortie, signal_col):
    if data.is_file():
        return data
    if not data.is_dir():
        sys.exit(f"--data path not found: {data}")
    cands = sorted(set(data.rglob("*.json")) | set(data.rglob("*.csv"))
                   | set(data.rglob("*.parquet")) | set(data.rglob("*.pq")))
    # prefer: sortie match, then *hires*, then json over csv
    def score(p):
        s = str(p).lower()
        return (sortie.lower() in s if sortie else False, "hires" in s, p.suffix == ".json")
    cands.sort(key=score, reverse=True)
    inspected = []
    for p in cands:
        cols = _columns_of(p)
        inspected.append((p, cols))
        if any(signal_col.lower() == c.lower() or signal_col.lower() in c.lower() for c in cols):
            print(f"[info] selected {p}  (has '{signal_col}')")
            return p
    print(f"[error] No file under {data} exposes a '{signal_col}' key/column.")
    for p, cols in inspected[:30]:
        shown = ", ".join(cols[:10]) + (" ..." if len(cols) > 10 else "")
        print(f"  {p.name}: [{shown}]")
    sys.exit("Point --data at the file, or set --signal-col.")


def load_series(path, signal_col, time_col, time_scale):
    if path.suffix.lower() == ".json":
        t, y = load_json_series(path, signal_col, time_col)
    else:
        t, y = load_tabular_series(path, signal_col, time_col)
    return t * time_scale, y


# ----------------------------------------------------------------------------- analysis
def first_sustained_below(time, sig, thr, min_dur):
    """Index of the start of the first run where sig < thr for >= min_dur seconds."""
    below = sig < thr
    i, n = 0, len(below)
    while i < n:
        if below[i]:
            j = i
            while j + 1 < n and below[j + 1]:
                j += 1
            if time[j] - time[i] >= min_dur:
                return i
            i = j + 1
        else:
            i += 1
    return None


def excursions_above(time, sig, thr):
    above = sig >= thr
    out, i, n = [], 0, len(above)
    while i < n:
        if above[i]:
            j = i
            while j + 1 < n and above[j + 1]:
                j += 1
            out.append((time[i], time[j], time[j] - time[i], float(np.max(sig[i:j + 1]))))
            i = j + 1
        else:
            i += 1
    return out


def analyze_series(time, sig, threshold, on_time, climbout_peak, climbout_dwell):
    """Return analysis dict, or None if radalt never crosses below threshold."""
    if not (sig < threshold).any():
        return None
    s = first_sustained_below(time, sig, threshold, on_time)
    if s is None:
        s = int(np.argmax(sig < threshold))
    exc = excursions_above(time[s:], sig[s:], threshold)
    dither, climbouts = [], []
    for e in exc:
        _t0, _t1, dur, peak = e
        is_climb = (peak - threshold) >= climbout_peak or dur >= climbout_dwell
        (climbouts if is_climb else dither).append(e)
    return {"start": s, "exc": exc, "dither": dither,
            "climbouts": climbouts, "n_approaches": len(climbouts) + 1}


def discover_sorties(root, signal_col):
    """Map sortie-key -> chosen file (prefer *hires*) for every folder exposing the signal."""
    files = (list(root.rglob("*.json")) + list(root.rglob("*.csv"))
             + list(root.rglob("*.parquet")) + list(root.rglob("*.pq")))
    groups = {}
    for p in files:
        cols = _columns_of(p)
        if any(signal_col.lower() == c.lower() or signal_col.lower() in c.lower() for c in cols):
            key = p.parent.name if p.parent != root else p.stem
            groups.setdefault(key, []).append(p)
    selected = {}
    for key, ps in groups.items():
        hires = [p for p in ps if "hires" in p.name.lower()]
        selected[key] = hires[0] if hires else sorted(ps)[0]
    return selected


def run_batch(args):
    sorties = discover_sorties(args.data, args.signal_col)
    if not sorties:
        sys.exit(f"No sortie files exposing '{args.signal_col}' under {args.data}")
    print(f"[info] {len(sorties)} sortie file(s) found\n")
    print(f"{'sortie':30} {'passes':>6} {'dither':>6} {'maxDwell':>9} {'maxPeak':>8}")
    rows, all_peaks, all_dwells = [], [], []
    for key in sorted(sorties):
        p = sorties[key]
        try:
            time, sig = load_series(p, args.signal_col, args.time_col, args.time_scale)
        except (SystemExit, Exception) as e:
            print(f"{key:30} {('(skip: ' + str(e)[:40] + ')'):>40}")
            continue
        res = analyze_series(time, sig, args.threshold, args.on_time,
                             args.climbout_peak, args.climbout_dwell)
        if not res or not res["dither"]:
            print(f"{key:30} {'-':>6} {'0':>6} {'-':>9} {'-':>8}")
            continue
        peaks = [e[3] for e in res["dither"]]
        dwells = [e[2] for e in res["dither"]]
        all_peaks += peaks
        all_dwells += dwells
        rows.append((key, max(peaks)))
        print(f"{key:30} {res['n_approaches']:>6} {len(peaks):>6} "
              f"{max(dwells):>9.2f} {max(peaks):>8.1f}")

    if not all_peaks:
        sys.exit("\nNo dither excursions found across any sortie.")
    arr = np.array(all_peaks)
    pk_max = float(arr.max())
    q = lambda v: float(np.percentile(arr, v))
    release = float(np.ceil((pk_max + args.release_margin) / args.release_round) * args.release_round)
    worst = max(rows, key=lambda r: r[1])

    print("\n--- Aggregate dither-excursion peaks (all sorties) ---")
    print(f"  sorties with dither : {len(rows)}")
    print(f"  excursions          : {len(arr)}")
    print(f"  peak ft  min/median/p90/p95/max : "
          f"{arr.min():.0f} / {q(50):.0f} / {q(90):.0f} / {q(95):.0f} / {pk_max:.0f}")
    print(f"  max dither dwell    : {max(all_dwells):.2f} s")
    print(f"  worst sortie        : {worst[0]} (peak {worst[1]:.1f} ft)")

    print(f"\n--- Release threshold (Path C: insert < {args.threshold:.0f} ft, "
          f"release > release_thr, hold in band) ---")
    print(f"  max dither peak (all sorties) : {pk_max:.1f} ft")
    print(f"  + release margin              : {args.release_margin:.0f} ft")
    print(f"  RECOMMENDED AptTrimBiasAltThreshRelease : {release:.0f} ft "
          f"(rounded to {args.release_round:.0f} ft)")
    print(f"  hysteresis band               : {release - args.threshold:.0f} ft")
    if release <= args.threshold:
        print("  [warn] release <= insert threshold — check inputs")


def run_single(args):
    f = find_data_file(args.data, args.sortie, args.signal_col)
    if args.list_signals:
        print("Keys/columns:", ", ".join(_columns_of(f)))
        return
    time, sig = load_series(f, args.signal_col, args.time_col, args.time_scale)
    print(f"[info] {len(sig)} samples; t = {time[0]:.2f}..{time[-1]:.2f} s; "
          f"radAlt = {np.min(sig):.0f}..{np.max(sig):.0f} ft")
    if not (sig < args.threshold).any():
        sys.exit(f"radAltVoted never drops below {args.threshold} ft — check threshold/units.")
    res = analyze_series(time, sig, args.threshold, args.on_time,
                         args.climbout_peak, args.climbout_dwell)
    s = res["start"]
    print(f"[info] approach window anchored below {args.threshold:.0f} ft at t = {time[s]:.2f} s")
    dither, climbouts = res["dither"], res["climbouts"]
    print(f"\nDetected ~{res['n_approaches']} approach pass(es): {len(climbouts)} climb-out(s).")
    if climbouts:
        print("\nGenuine climb-outs (NOT dither):")
        print(f"  {'t_start(s)':>13}  {'dwell(s)':>10}  {'peak(ft)':>9}")
        for t0, _t1, dur, peak in climbouts:
            print(f"  {t0:>13.2f}  {dur:>10.2f}  {peak:>9.1f}")
    print("\nIntra-approach drop-outs (dither):")
    print(f"  {'#':>2}  {'t_start(s)':>13}  {'dwell(s)':>9}  {'peak(ft)':>9}")
    for k, (t0, _t1, dur, peak) in enumerate(dither, 1):
        print(f"  {k:>2}  {t0:>13.2f}  {dur:>9.3f}  {peak:>9.1f}")
    if not dither:
        print("  (none)\n\nNo intra-approach dither — not constrained by this sortie.")
        return
    durs = np.array([e[2] for e in dither])
    peaks = np.array([e[3] for e in dither])
    deepest = float(peaks.max() - args.threshold)
    release = float(np.ceil((peaks.max() + args.release_margin) / args.release_round) * args.release_round)
    print("\n--- Sizing (this sortie) ---")
    print(f"  max dither dwell  : {durs.max():.3f} s")
    print(f"  max dither peak   : {peaks.max():.1f} ft  ({deepest:.0f} ft above threshold)")
    print(f"  Path A offTime    : {np.ceil(durs.max() + args.margin):.0f} s  (masks real excursions; not recommended)")
    print(f"  Path C release    : {release:.0f} ft  (AptTrimBiasAltThreshRelease; single-sortie -- use --batch for fleet)")


def load_named(path, col, time_col, time_scale):
    if path.suffix.lower() == ".json":
        t, y = load_json_series(path, col, time_col)
    else:
        t, y = load_tabular_series(path, col, time_col)
    return t * time_scale, y


def sustained_below_starts(time, sig, thr, min_dur):
    """Start index of every maximal below-threshold run lasting >= min_dur (one per approach)."""
    below = sig < thr
    starts, i, n = [], 0, len(below)
    while i < n:
        if below[i]:
            j = i
            while j + 1 < n and below[j + 1]:
                j += 1
            if time[j] - time[i] >= min_dur:
                starts.append(i)
            i = j + 1
        else:
            i += 1
    return starts


def _peak_slew_over_dt(seg_t, seg_y, slew_dt):
    """Max |dy|/dt over windows of at least slew_dt seconds (robust to 1-sample spikes)."""
    n = len(seg_t)
    best, j = 0.0, 0
    for i in range(n):
        if j < i + 1:
            j = i + 1
        while j < n and seg_t[j] - seg_t[i] < slew_dt:
            j += 1
        if j >= n:
            break
        dt = seg_t[j] - seg_t[i]
        if dt > 0:
            best = max(best, abs(seg_y[j] - seg_y[i]) / dt)
    return float(best)


def measure_insertion_transients(event_times, tm, ym, win, slew_dt):
    """For each event, return (step, peak_slew) of (tm, ym) over [te, te+win]."""
    out = []
    for te in event_times:
        i0 = int(np.searchsorted(tm, te))
        i1 = int(np.searchsorted(tm, te + win))
        if i1 - i0 < 2:
            continue
        seg_t, seg_y = tm[i0:i1 + 1], ym[i0:i1 + 1]
        step = float(seg_y.max() - seg_y.min())
        slew = _peak_slew_over_dt(seg_t, seg_y, slew_dt)
        out.append((step, slew))
    return out


def run_fade_check(args):
    sorties = discover_sorties(args.data, args.signal_col)
    if not sorties:
        sys.exit(f"No sortie files exposing '{args.signal_col}' under {args.data}")
    print(f"[info] {len(sorties)} sortie file(s); measuring '{args.measure_col}' at insertion events\n")
    print(f"{'sortie':30} {'events':>6} {'maxStep':>9} {'maxSlew/s':>10}")
    rows, steps_all, slews_all = [], [], []
    for key in sorted(sorties):
        p = sorties[key]
        try:
            t, sig = load_named(p, args.signal_col, args.time_col, args.time_scale)
            tm, ym = load_named(p, args.measure_col, args.time_col, args.time_scale)
        except (SystemExit, Exception) as e:
            print(f"{key:30} {('(skip: ' + str(e)[:36] + ')'):>40}")
            continue
        starts = sustained_below_starts(t, sig, args.threshold, args.on_time)
        if not starts:
            print(f"{key:30} {'0':>6} {'-':>9} {'-':>10}")
            continue
        ev = [t[i] for i in starts]
        tr = measure_insertion_transients(ev, tm, ym, args.fade_window, args.slew_dt)
        if not tr:
            print(f"{key:30} {len(ev):>6} {'-':>9} {'-':>10}")
            continue
        st = [a for a, _ in tr]
        sl = [b for _, b in tr]
        steps_all += st
        slews_all += sl
        rows.append((key, max(st)))
        print(f"{key:30} {len(tr):>6} {max(st):>9.2f} {max(sl):>10.2f}")

    if not steps_all:
        sys.exit(f"\nNo transients measured (is '{args.measure_col}' present? try --list-signals).")
    steps, slews = np.array(steps_all), np.array(slews_all)
    q = lambda a, v: float(np.percentile(a, v))
    worst = max(rows, key=lambda r: r[1])
    fade_slew = q(steps, 95) / args.fade_target
    print(f"\n--- Insertion transient ({args.measure_col}, window {args.fade_window:.1f}s, "
          f"slew over {args.slew_dt:.2f}s, {len(steps)} events) ---")
    print(f"  step  min/median/p90/p95/max : "
          f"{steps.min():.2f} / {q(steps,50):.2f} / {q(steps,90):.2f} / {q(steps,95):.2f} / {steps.max():.2f}")
    print(f"  peak slew/s  p50/p95/max     : {q(slews,50):.2f} / {q(slews,95):.2f} / {slews.max():.2f}")
    print(f"  worst sortie                 : {worst[0]} (step {worst[1]:.2f})")
    print(f"\n--- Fade recommendation (spread p95 step over fade_target) ---")
    print(f"  fade target time             : {args.fade_target:.2f} s")
    print(f"  implied AptTrimBiasFadeRate  : {fade_slew:.2f} {args.measure_col}-units/s")
    if q(slews, 95) <= fade_slew * 1.2:
        print("  NOTE: current onset slew already near the faded rate; fade may add little here.")
    else:
        print("  Current onset is much steeper than the faded rate -> fade meaningfully smooths it.")


def main():
    ap = argparse.ArgumentParser(description="Size APT bias release threshold / offTime from radAltVoted.")
    ap.add_argument("--data", required=True, type=Path)
    ap.add_argument("--sortie", default=None)
    ap.add_argument("--batch", action="store_true",
                    help="Sweep all sortie folders under --data and aggregate excursion peaks")
    ap.add_argument("--threshold", type=float, default=500.0, help="pdiAptTrimBiasAltThresh (ft)")
    ap.add_argument("--margin", type=float, default=1.0, help="offTime margin (s, Path A)")
    ap.add_argument("--release-margin", type=float, default=25.0,
                    help="ft added above worst dither peak to set the release threshold (Path C)")
    ap.add_argument("--release-round", type=float, default=10.0, help="round release threshold to nearest N ft")
    ap.add_argument("--climbout-peak", type=float, default=150.0,
                    help="ft above threshold above which an excursion is a genuine climb-out")
    ap.add_argument("--climbout-dwell", type=float, default=30.0,
                    help="seconds above which an excursion is a genuine climb-out")
    ap.add_argument("--on-time", type=float, default=1.0)
    ap.add_argument("--signal-col", default="radAltVoted")
    ap.add_argument("--time-col", default="time")
    ap.add_argument("--time-scale", type=float, default=1.0)
    ap.add_argument("--fade-check", action="store_true",
                    help="Characterize the insertion transient of --measure-col across all sorties")
    ap.add_argument("--measure-col", default="pitchServoTorqueFilt",
                    help="signal whose insertion transient to measure (e.g. aptBias, pitchServoTorqueFilt)")
    ap.add_argument("--fade-window", type=float, default=2.0, help="seconds after insertion to measure")
    ap.add_argument("--fade-target", type=float, default=1.0, help="target fade-in time for recommendation")
    ap.add_argument("--slew-dt", type=float, default=0.1,
                    help="min interval (s) over which peak slew is measured; rejects 1-sample spikes")
    ap.add_argument("--list-signals", action="store_true")
    args = ap.parse_args()

    if args.fade_check:
        run_fade_check(args)
    elif args.batch:
        run_batch(args)
    else:
        run_single(args)


if __name__ == "__main__":
    main()
