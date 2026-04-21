"""
analyze_iads.py — parallel IADS CSV analyzer
Handles multi-GB files using multiprocessing row-chunk parallelism.
Produces a compact analysis.json the browser diagnostic agent can load.

Usage:
  python analyze_iads.py <csv_or_zip_path> [--out analysis.json] [--workers N]

Examples:
  python analyze_iads.py flight_data.csv
  python analyze_iads.py AFCS_del3_v20260323_S119N208B_2.zip
  python analyze_iads.py flight_data.csv --out my_analysis.json --workers 8

Notes:
  - Transitions are recorded for bool (0/1) and discrete-enum (integer ≤64 unique
    values) signals only. Continuous float signals contribute stats (min/max/mean)
    but no transitions.
  - Up to N-1 transitions may be missed at chunk boundaries (one per worker seam).
"""

import csv
import json
import sys
import os
import glob
import shutil
import zipfile
import math
import argparse
import tempfile
import time
import multiprocessing
from multiprocessing.pool import ThreadPool
from datetime import datetime
from collections import deque

ENUM_MAX = 64   # max distinct integer values before treating a channel as continuous

_TIME_EXACT = {
    "t", "time", "timestamp", "time_s", "time_sec", "timesec",
    "rel_time", "reltime", "irig", "irig_time",
    "elapsed", "elapsed_s", "elapsed_time",
    "run_time", "runtime", "mission_time", "missiontime",
    "test_time", "testtime", "t_sec", "tsec",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_time_to_s(raw):
    """Normalize a time value to float seconds.

    Handles:
      - Plain float / int strings      (pass-through, e.g. IRIG as decimal seconds)
      - "HH:MM:SS[.sss]"               UTC colon-separated → seconds since midnight
      - "DDD:HH:MM:SS[.sss]"           IADS day-of-year format → total seconds
      - "HHMMSS[.sss]"                 6-digit packed UTC → seconds since midnight
    Returns None on failure.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    try:
        return float(s)
    except ValueError:
        pass
    parts = s.split(":")
    if len(parts) == 3:
        try:
            h, m, sec = int(parts[0]), int(parts[1]), float(parts[2])
            return float(h * 3600 + m * 60 + sec)
        except (ValueError, TypeError):
            pass
    # DDD:HH:MM:SS.sss  (IADS day-of-year format, e.g. "102:12:21:46.800")
    if len(parts) == 4:
        try:
            day, h, m, sec = int(parts[0]), int(parts[1]), int(parts[2]), float(parts[3])
            return float(day * 86400 + h * 3600 + m * 60 + sec)
        except (ValueError, TypeError):
            pass
    if len(s) >= 6:
        try:
            f = float(s)
            hi = int(f) // 10000
            mi = (int(f) % 10000) // 100
            si = f - int(f) // 100 * 100
            if 0 <= hi < 24 and 0 <= mi < 60 and 0 <= si < 60:
                return float(hi * 3600 + mi * 60 + si)
        except (ValueError, TypeError):
            pass
    return None


def _find_time_col(headers):
    for i, h in enumerate(headers):
        hl = h.lower().strip()
        if (hl in _TIME_EXACT
                or hl.endswith("_t")
                or hl.endswith("_time")
                or hl.endswith("_sec")
                or hl.endswith("_irig")
                or hl.startswith("time_")
                or hl.startswith("irig_")):
            return i
    return -1


def _chunk_boundaries(file_path, n):
    """Return list of (byte_start, byte_end) tuples covering data rows (after header).

    All start positions are row-aligned so workers need no skip logic.
    """
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        f.readline()            # consume header
        data_start = f.tell()
        data_size  = file_size - data_start

        split_points = [data_start]
        for i in range(1, n):
            target = data_start + i * data_size // n
            f.seek(target)
            f.readline()        # advance to start of next complete row
            split_points.append(f.tell())
        split_points.append(file_size)

    return [(split_points[i], split_points[i + 1]) for i in range(n)]


def _extract_sortie(paths):
    """Extract sortie + leg label from ZIP filenames.

    Matches the pattern _SXXXNYYY_L (e.g. S102N208B_1) and returns 'S102_1'.
    Falls back to bare SXXX if no leg number is present (e.g. 'S119').
    """
    import re
    for p in paths:
        name = os.path.basename(p)
        # S = Sortie, G = Ground  e.g. S102N208B_1, G034N208B
        m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})[A-Z][A-Z0-9]*_(\d+)', name, re.IGNORECASE)
        if m:
            return f"{m.group(1).upper()}_{m.group(2)}"
        # Fallback: bare SXXX/GXXX with no tail/leg
        m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})(?!\d)', name, re.IGNORECASE)
        if m:
            return m.group(1).upper()
    return None


def _extract_tail(paths):
    """Extract aircraft tail number from ZIP filenames (e.g. S107N208B_2.zip -> N208B)."""
    import re
    for p in paths:
        name = os.path.basename(p)
        m = re.search(r'(?<![A-Za-z])[SG]\d{2,5}([A-Z][A-Z0-9]+)(?:_\d+)?\.zip', name, re.IGNORECASE)
        if m:
            return m.group(1).upper()
    return None


def _add_sortie_suffix(out_path, sortie):
    """Insert _SXXX_L before the .json extension: analysis.json → analysis_S102_1.json."""
    if not sortie:
        return out_path
    root, ext = os.path.splitext(out_path)
    return f"{root}_{sortie}{ext}"


def sanitize_for_json(obj):
    """Recursively replace non-finite floats with None so json.dump(allow_nan=False) never raises."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# Worker (must be a top-level function for multiprocessing spawn on Windows)
# ---------------------------------------------------------------------------

def _worker(args):
    """Process one byte-range chunk of a CSV file and return partial stats."""
    file_path, byte_start, byte_end, headers, time_col_idx, worker_id, n_workers, plot_col_map = args
    n = len(headers)

    col_min          = [math.inf]  * n
    col_max          = [-math.inf] * n
    col_sum          = [0.0]       * n
    col_count        = [0]         * n
    col_has_non_bool = [False]     * n
    col_is_integer   = [True]      * n
    col_unique       = [set()      for _ in range(n)]
    col_prev         = [None]      * n

    transitions          = []
    total_rows           = 0
    t_min                = math.inf
    t_max                = -math.inf
    sample_head          = []
    tail_buf             = deque(maxlen=12)
    first_transition_row = -1
    context_buf          = []
    first_vals           = [None]  * n   # first finite float seen per column
    last_time_val        = None          # time string of the last row processed
    plot_series          = {sig: [] for sig in plot_col_map}  # sig → [(t_val, v), ...]

    with open(file_path, "rb") as f:
        f.seek(byte_start)
        while True:
            if f.tell() >= byte_end:
                break
            raw_line = f.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
            if not line:
                continue
            try:
                row = next(csv.reader([line]))
            except StopIteration:
                continue

            total_rows += 1
            nj = min(n, len(row))

            # worker 0 owns the first rows for sample_head
            if worker_id == 0 and total_rows <= 10:
                sample_head.append(row[:])
            tail_buf.append(row[:])

            # parse time once per row
            if time_col_idx >= 0 and time_col_idx < len(row):
                t_parsed = parse_time_to_s(row[time_col_idx])
                if t_parsed is not None and math.isfinite(t_parsed):
                    t_float = t_parsed
                    if t_parsed < t_min: t_min = t_parsed
                    if t_parsed > t_max: t_max = t_parsed
                else:
                    t_float = None
                t_val = str(t_parsed) if t_parsed is not None else str(total_rows)
            else:
                t_float = None
                t_val = str(total_rows)
            last_time_val = t_val

            # collect plot-signal samples (store as float seconds for efficient slicing)
            for sig, col_idx in plot_col_map.items():
                if col_idx < nj and t_float is not None:
                    try:
                        pv = float(row[col_idx])
                        if math.isfinite(pv):
                            plot_series[sig].append((t_float, pv))
                    except (ValueError, TypeError):
                        pass

            # single merged loop: stats + transitions
            for j in range(nj):
                try:
                    v = float(row[j])
                except (ValueError, TypeError):
                    continue
                if not math.isfinite(v):
                    continue

                if first_vals[j] is None:
                    first_vals[j] = v

                # --- stats ---
                if v < col_min[j]: col_min[j] = v
                if v > col_max[j]: col_max[j] = v
                col_sum[j]   += v
                col_count[j] += 1
                if v != 0.0 and v != 1.0:
                    col_has_non_bool[j] = True
                if col_is_integer[j]:
                    if v % 1.0 != 0.0:
                        col_is_integer[j] = False
                    elif len(col_unique[j]) <= ENUM_MAX:
                        col_unique[j].add(v)

                # --- transitions (bool / enum only) ---
                prev = col_prev[j]
                if prev is not None and prev != v:
                    is_bool = not col_has_non_bool[j]
                    is_enum = col_is_integer[j] and 0 < len(col_unique[j]) <= ENUM_MAX
                    if is_bool or is_enum:
                        transitions.append({
                            "signal": headers[j],
                            "time":   t_val,
                            "from":   prev,
                            "to":     v,
                            "row_idx": total_rows - 1,
                        })
                        if first_transition_row == -1:
                            first_transition_row = total_rows - 1
                col_prev[j] = v

            # collect context rows around the first transition in this chunk
            if first_transition_row >= 0:
                dist = (total_rows - 1) - first_transition_row
                if -5 <= dist <= 5:
                    obj = dict(zip(headers, row))
                    obj["_is_transition"] = ((total_rows - 1) == first_transition_row)
                    context_buf.append(obj)

    return {
        "col_min":           col_min,
        "col_max":           col_max,
        "col_sum":           col_sum,
        "col_count":         col_count,
        "col_has_non_bool":  col_has_non_bool,
        "col_is_integer":    col_is_integer,
        "col_unique":        col_unique,
        "transitions":       transitions,
        "t_min":             t_min,
        "t_max":             t_max,
        "total_rows":        total_rows,
        "sample_head":       sample_head,
        "sample_tail":       list(tail_buf),
        "first_transition_row": first_transition_row,
        "context_buf":       context_buf,
        "first_vals":        first_vals,
        "last_vals":         list(col_prev),
        "last_time_val":     last_time_val,
        "plot_series":       plot_series,
    }


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def _merge(partials, headers, time_col_idx, filename):
    """Merge partial results from all workers into the final analysis dict."""
    n = len(headers)
    col_min          = [math.inf]  * n
    col_max          = [-math.inf] * n
    col_sum          = [0.0]       * n
    col_count        = [0]         * n
    col_has_non_bool = [False]     * n
    col_is_integer   = [True]      * n
    col_unique       = [set()      for _ in range(n)]

    total_rows    = 0
    t_min         = math.inf
    t_max         = -math.inf
    all_trans     = []
    sample_head   = []
    sample_tail   = []
    best_ctx_row  = math.inf
    context_buf   = []
    plot_series   = {}   # sig → sorted [(t_val, v), ...]

    for p in partials:
        for j in range(n):
            if p["col_min"][j]  < col_min[j]:  col_min[j]  = p["col_min"][j]
            if p["col_max"][j]  > col_max[j]:  col_max[j]  = p["col_max"][j]
            col_sum[j]   += p["col_sum"][j]
            col_count[j] += p["col_count"][j]
            if p["col_has_non_bool"][j]: col_has_non_bool[j] = True
            if not p["col_is_integer"][j]: col_is_integer[j] = False
            col_unique[j].update(p["col_unique"][j])

        total_rows += p["total_rows"]
        if p["t_min"] < t_min: t_min = p["t_min"]
        if p["t_max"] > t_max: t_max = p["t_max"]
        all_trans.extend(p["transitions"])

        if p["sample_head"]:
            sample_head = p["sample_head"]   # worker 0 owns this
        if p["sample_tail"]:
            sample_tail = p["sample_tail"]   # last worker with data wins

        if 0 <= p["first_transition_row"] < best_ctx_row:
            best_ctx_row = p["first_transition_row"]
            context_buf  = p["context_buf"]

        for sig, pts in p.get("plot_series", {}).items():
            if sig not in plot_series:
                plot_series[sig] = []
            plot_series[sig].extend(pts)

    # Cross-chunk boundary transitions
    # Uses merged channel classification so the bool/enum check is globally correct.
    for k in range(len(partials) - 1):
        pk   = partials[k]
        pk1  = partials[k + 1]
        bnd_time = pk["last_time_val"] or str(sum(p["total_rows"] for p in partials[:k + 1]))
        for j in range(n):
            last_v  = pk["last_vals"][j]
            first_v = pk1["first_vals"][j]
            if last_v is None or first_v is None or last_v == first_v:
                continue
            is_bool = not col_has_non_bool[j]
            is_enum = col_is_integer[j] and 0 < len(col_unique[j]) <= ENUM_MAX
            if is_bool or is_enum:
                all_trans.append({
                    "signal":  headers[j],
                    "time":    bnd_time,
                    "from":    last_v,
                    "to":      first_v,
                    "row_idx": -1,   # seam transition — no single source row
                })

    try:
        all_trans.sort(key=lambda x: float(x["time"]))
    except (ValueError, TypeError):
        pass

    # Sort plot series by time (chunks are ordered but seams may interleave)
    for sig in plot_series:
        plot_series[sig].sort(key=lambda pt: pt[0])

    bool_channels = []
    enum_channels = []
    num_channels  = []
    for j, h in enumerate(headers):
        if col_count[j] == 0:
            continue
        if not col_has_non_bool[j] and col_min[j] >= 0 and col_max[j] <= 1:
            bool_channels.append({
                "name":         h,
                "active_count": int(col_sum[j]),
                "total_count":  col_count[j],
                "active_pct":   round(col_sum[j] / col_count[j] * 100, 1),
            })
        elif col_is_integer[j] and 0 < len(col_unique[j]) <= ENUM_MAX:
            enum_channels.append({
                "name":   h,
                "values": sorted(int(v) for v in col_unique[j]),
            })
        else:
            num_channels.append({
                "name": h,
                "min":  round(col_min[j], 4) if math.isfinite(col_min[j]) else None,
                "max":  round(col_max[j], 4) if math.isfinite(col_max[j]) else None,
                "mean": (round(col_sum[j] / col_count[j], 4)
                         if col_count[j] and math.isfinite(col_sum[j] / col_count[j])
                         else None),
            })

    duration = round(t_max - t_min, 2) if math.isfinite(t_min) and math.isfinite(t_max) else None
    time_col = headers[time_col_idx] if time_col_idx >= 0 else None

    return {
        "filename":      filename,
        "generated_at":  datetime.now().isoformat(),
        "total_rows":    total_rows,
        "time_col":      time_col,
        "duration_s":    duration,
        "bool_channels": bool_channels,
        "enum_channels": enum_channels,
        "num_channels":  num_channels,
        "transitions":   all_trans,
        "context_rows":  context_buf,
        "sample_head":   [dict(zip(headers, r)) for r in sample_head],
        "sample_tail":   [dict(zip(headers, r)) for r in sample_tail],
        "headers":       headers,
        "_plot_series":  plot_series,   # removed before JSON output, used for episode plots
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# Match both bare names and the 'Enum'-suffixed forms that appear in IADS CSVs
# e.g. FCC1A.g_fgaltcontrollaw_mdlrefdw.rtb.vertActiveEnum
_MODE_SUFFIXES = {
    'latActive', 'vertActive', 'atActive',
    'latActiveEnum', 'vertActiveEnum', 'atActiveEnum',
}

def _signal_matches(name, pattern):
    """Match by exact name or by the last dot-separated component (IADS suffix)."""
    return name == pattern or name.split(".")[-1] == pattern


def _save_mode_transitions(result):
    """
    Extract transitions for latActive / vertActive / atActive from the flat
    transition list and store them as result["mode_transitions"] BEFORE
    _extract_episodes consumes the list.  Safe to call multiple times.
    """
    trans = result.get("transitions", [])
    result["mode_transitions"] = [
        t for t in trans
        if t.get("signal", "").split(".")[-1] in _MODE_SUFFIXES
    ]


def _extract_episodes(result, signal, from_val, to_val):
    """
    Scan the sorted transition list for every occurrence of signal going
    from_val→to_val.  Each such event opens an episode; the episode closes
    when the same signal transitions back (to_val→from_val).

    result["transitions"] is replaced by result["episodes"], a list of:
      {
        "episode":     int,
        "start_time":  str,
        "end_time":    str | null,   # null if flight ended before recovery
        "duration_s":  float | null,
        "transitions": [ ... ]       # all transitions during this episode
      }

    The trigger transition itself is the first entry of each episode's list.
    """
    trans = result.pop("transitions", [])

    episodes  = []
    ep_num    = 0
    i         = 0

    while i < len(trans):
        t = trans[i]
        try:
            is_trigger = (_signal_matches(t["signal"], signal)
                          and float(t["from"]) == from_val
                          and float(t["to"])   == to_val)
        except (ValueError, TypeError):
            is_trigger = False

        if not is_trigger:
            i += 1
            continue

        ep_num    += 1
        start_time = t["time"]
        end_time   = None
        j          = i + 1

        # Look back up to PRE_WINDOW_S before the trigger to capture leading indicators
        PRE_WINDOW_S = 5.0
        try:
            t_trigger = float(t["time"])
            pre_trans = []
            k = i - 1
            while k >= 0:
                try:
                    t_k = float(trans[k]["time"])
                except (ValueError, TypeError):
                    k -= 1
                    continue
                if t_trigger - t_k > PRE_WINDOW_S:
                    break
                pre_trans.append(trans[k])
                k -= 1
            pre_trans.reverse()   # chronological order
        except (ValueError, TypeError):
            pre_trans = []

        ep_trans = pre_trans + [t]

        while j < len(trans):
            t2 = trans[j]
            ep_trans.append(t2)
            try:
                is_recovery = (t2["signal"] == t["signal"]   # same exact signal that triggered
                               and float(t2["from"]) == to_val
                               and float(t2["to"])   == from_val)
            except (ValueError, TypeError):
                is_recovery = False

            if is_recovery:
                end_time = t2["time"]
                j += 1   # include recovery row, then stop
                break
            j += 1

        try:
            dur = round(float(end_time) - float(start_time), 2) if end_time else None
        except (ValueError, TypeError):
            dur = None

        episodes.append({
            "episode":     ep_num,
            "start_time":  start_time,
            "end_time":    end_time,
            "duration_s":  dur,
            "transitions": ep_trans,
        })

        i = j   # resume outer scan after the end of this episode

    result["trigger"]  = {"signal": signal, "from": from_val, "to": to_val}
    result["episodes"] = episodes
    return result


def _attach_episode_plots(result, plot_series):
    """Slice the global plot_series to each episode's time window, downsample, and attach."""
    import bisect
    MAX_PTS  = 500
    MARGIN_S = 30.0

    # Pre-extract sorted time keys for fast binary-search slicing
    sig_times = {sig: [pt[0] for pt in pts] for sig, pts in plot_series.items()}

    for ep in result.get("episodes", []):
        try:
            t_start = float(ep["start_time"])
            t_end   = float(ep["end_time"]) if ep.get("end_time") else t_start
        except (TypeError, ValueError):
            ep["plots"] = {}
            continue

        lo = t_start - MARGIN_S
        hi = t_end   + MARGIN_S
        ep_plots = {}

        for sig, pts in plot_series.items():
            times   = sig_times[sig]
            lo_idx  = bisect.bisect_left(times,  lo)
            hi_idx  = bisect.bisect_right(times, hi)
            sliced  = pts[lo_idx:hi_idx]

            # Uniform downsample to at most MAX_PTS
            if len(sliced) > MAX_PTS:
                step   = len(sliced) / MAX_PTS
                sliced = [sliced[int(i * step)] for i in range(MAX_PTS)]

            ep_plots[sig] = [[pt[0], pt[1]] for pt in sliced]

        ep["plots"] = ep_plots


def _phase_window(mode_transitions, phase_vals):
    """Return (t_lo, t_hi) spanning the earliest activation to the latest
    activation-or-deactivation of any mode named in *phase_vals*.
    Returns (None, None) when no matching transitions are found.
    """
    _LAT_NAMES  = ['standby', 'heading', 'nav', 'navAppr', 'takeoff', 'align', 'track']
    _VERT_NAMES = ['standby', 'pitch', 'verticalSpeed', 'altitudeHold',
                   'altitudeSelect', 'glidePath', 'vnav', 'takeoff', 'flare', 'vgp', 'flc']
    _AT_NAMES   = ['standby', 'takeOff', 'speed', 'retard', 'lim', 'thrust']
    _EMAP = {
        'latActiveEnum':  _LAT_NAMES,  'latActive':  _LAT_NAMES,
        'vertActiveEnum': _VERT_NAMES, 'vertActive': _VERT_NAMES,
        'atActiveEnum':   _AT_NAMES,   'atActive':   _AT_NAMES,
    }
    t_lo = t_hi = None
    for t in mode_transitions:
        names = _EMAP.get(t.get("signal", "").split(".")[-1])
        if names is None:
            continue
        try:
            to_name   = names[int(t.get("to",   -1))]
            from_name = names[int(t.get("from", -1))]
        except (IndexError, ValueError, TypeError):
            to_name   = str(t.get("to",   ""))
            from_name = str(t.get("from", ""))
        ts = float(t["time"])
        if to_name in phase_vals:
            if t_lo is None or ts < t_lo: t_lo = ts
        if to_name in phase_vals or from_name in phase_vals:
            if t_hi is None or ts > t_hi: t_hi = ts
    return t_lo, t_hi


_FLIGHT_MAX_PTS = 2000
_HIRES_MAX_PTS  = 8000


def _downsample_pts(pts, max_pts):
    if len(pts) <= max_pts:
        return pts
    # Min-max downsampling: split into max_pts/2 buckets, keep both the min-value
    # and max-value point from each bucket (in time order).  Preserves all peaks
    # and valleys at the same output size — far superior to uniform stride for
    # detecting deviations and spikes.
    bucket_count = max(1, max_pts // 2)
    step = len(pts) / bucket_count
    out = []
    for i in range(bucket_count):
        lo = int(i * step)
        hi = int((i + 1) * step)
        bucket = pts[lo:hi]
        if not bucket:
            continue
        mn = min(bucket, key=lambda p: p[1])
        mx = max(bucket, key=lambda p: p[1])
        if mn[0] <= mx[0]:
            out.append(mn)
            if mn is not mx:
                out.append(mx)
        else:
            out.append(mx)
            if mn is not mx:
                out.append(mn)
    return out


def _save_flight_plots(result, plot_series, max_pts=None):
    """Save approach/landing window → result['flight_plots'], capped at max_pts per signal.

    Window: earliest approach/landing mode activation − 10 s through latest
    landing-mode DEACTIVATION + 5 s (captures full post-touchdown ground roll).
    Falls back to the full-flight dataset if no matching transitions are found.
    """
    if max_pts is None:
        max_pts = _FLIGHT_MAX_PTS
    phase_vals = {'navAppr', 'glidePath', 'align', 'flare', 'retard'}
    t_lo, t_hi = _phase_window(result.get("mode_transitions", []), phase_vals)
    if t_lo is not None:
        t_lo -= 10
        t_hi += 5
    _GNSS_SIGS = {'GNSS_Latitude', 'GNSS_Latitude_Fine', 'GNSS_Longitude', 'GNSS_Longitude_Fine'}
    plots = {}
    for sig, pts in plot_series.items():
        if t_lo is not None:
            pts = [p for p in pts if t_lo <= p[0] <= t_hi]
        if sig in _GNSS_SIGS:
            pts = [p for p in pts if p[1] != 0.0]
        pts = _downsample_pts(pts, max_pts)
        plots[sig] = [[p[0], p[1]] for p in pts]
    result["flight_plots"] = plots


def _save_takeoff_plots(result, plot_series, max_pts=None):
    """Save takeoff window → result['takeoff_plots'], capped at max_pts per signal.

    Modes: latActive=takeoff (4), vertActive=takeoff (7), atActive=takeOff (1).
    Window: first activation − 10 s through last deactivation + 30 s.
    The +30 s post-margin captures the climb-out after modes revert to standby.
    Stores an empty dict when no takeoff modes are found.
    """
    if max_pts is None:
        max_pts = _FLIGHT_MAX_PTS
    phase_vals = {'takeoff', 'takeOff'}
    t_lo, t_hi = _phase_window(result.get("mode_transitions", []), phase_vals)
    if t_lo is None:
        result["takeoff_plots"] = {}
        return
    t_lo -= 10
    t_hi += 30
    _GNSS_SIGS = {'GNSS_Latitude', 'GNSS_Latitude_Fine', 'GNSS_Longitude', 'GNSS_Longitude_Fine'}
    plots = {}
    for sig, pts in plot_series.items():
        pts = [p for p in pts if t_lo <= p[0] <= t_hi]
        if sig in _GNSS_SIGS:
            pts = [p for p in pts if p[1] != 0.0]
        pts = _downsample_pts(pts, max_pts)
        plots[sig] = [[p[0], p[1]] for p in pts]
    result["takeoff_plots"] = plots


def _save_hires_file(out_path, result, plot_series):
    """Write a companion *_hires.json containing flight_plots + takeoff_plots at _HIRES_MAX_PTS."""
    root, ext = os.path.splitext(out_path)
    hires_path = f"{root}_hires{ext}"
    hires = {"mode_transitions": result.get("mode_transitions", [])}
    _save_flight_plots(hires, plot_series, max_pts=_HIRES_MAX_PTS)
    _save_takeoff_plots(hires, plot_series, max_pts=_HIRES_MAX_PTS)
    with open(hires_path, "w", encoding="utf-8") as f:
        json.dump(sanitize_for_json({"flight_plots": hires["flight_plots"],
                                     "takeoff_plots": hires["takeoff_plots"]}),
                  f, separators=(",", ":"), allow_nan=False)
    hires_kb = os.path.getsize(hires_path) / 1024
    print(f"  hires: {hires_path}  ({hires_kb:.0f} KB)")


def process_file(input_path, out_path, n_workers, trigger=None, trigger_from=1.0, trigger_to=0.0, plot_signals=None, keep_plots=False, quiet=False, trace_graph=None):
    t0 = time.perf_counter()

    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        print(f"ERROR: File not found: {input_path}")
        sys.exit(1)

    size_gb = os.path.getsize(input_path) / 1e9

    tmp_file = None
    try:
        is_zip = input_path.lower().endswith(".zip")
        if is_zip:
            with zipfile.ZipFile(input_path, "r") as z:
                csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                if not csv_files:
                    print(f"ERROR: No CSV found in ZIP. Contents: {z.namelist()}")
                    sys.exit(1)
                csv_name = csv_files[0]
                info     = z.getinfo(csv_name)
                uncomp_gb = info.file_size / 1e9
                if not quiet:
                    print(f"  extracting {csv_name}  ({uncomp_gb:.2f} GB)...")
                tmp_fd, tmp_file = tempfile.mkstemp(suffix=".csv")
                with os.fdopen(tmp_fd, "wb") as dst, z.open(csv_name) as src:
                    shutil.copyfileobj(src, dst)
                csv_path = tmp_file
                filename = csv_name
        else:
            csv_path = input_path
            filename = os.path.basename(input_path)

        # read headers (first line only)
        with open(csv_path, "rb") as f:
            header_line = f.readline().decode("utf-8", errors="replace").rstrip("\r\n")
        headers      = next(csv.reader([header_line]))
        time_col_idx = _find_time_col(headers)

        # Build plot_col_map: suffix-match each requested signal to a column index.
        # Only the FIRST matching column is used (break on match) so multi-lane signals
        # like FCC1A/FCC1B both having latDevSel resolve to a single column.
        plot_signals_list = [s.strip() for s in plot_signals.split(",") if s.strip()] if plot_signals else []
        plot_col_map = {}
        for sig in plot_signals_list:
            for j, h in enumerate(headers):
                if _signal_matches(h, sig):
                    plot_col_map[sig] = j
                    break            # first match only — skip duplicate lanes
            if sig not in plot_col_map and not quiet:
                print(f"  WARNING: plot signal '{sig}' not found in headers")
        if not quiet:
            print(f"  {len(headers)} cols  |  {len(plot_col_map)} plot signals  |  {n_workers} workers")

        boundaries  = _chunk_boundaries(csv_path, n_workers)
        worker_args = [
            (csv_path, start, end, headers, time_col_idx, i, n_workers, plot_col_map)
            for i, (start, end) in enumerate(boundaries)
        ]

        with multiprocessing.Pool(processes=n_workers) as pool:
            partials = pool.map(_worker, worker_args)

        result = _merge(partials, headers, time_col_idx, filename)
        plot_series = result.pop("_plot_series", {})

    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

    if trigger:
        _save_mode_transitions(result)
        result = _extract_episodes(result, trigger, trigger_from, trigger_to)
        if plot_series:
            _attach_episode_plots(result, plot_series)
            _save_flight_plots(result, plot_series)
            _save_takeoff_plots(result, plot_series)
    elif keep_plots and plot_series:
        # Directory mode: preserve raw plot series so the caller can merge and
        # attach plots after all files are combined and episodes are extracted.
        result["_plot_series"] = plot_series

    result["processing_time_s"] = round(time.perf_counter() - t0, 2)
    if trace_graph:
        result["trace_graph"] = trace_graph

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sanitize_for_json(result), f, separators=(",", ":"), allow_nan=False)

    if trigger and plot_series:
        _save_hires_file(out_path, result, plot_series)

    out_kb = os.path.getsize(out_path) / 1024
    if not quiet:
        if trigger:
            n_ep = len(result["episodes"])
            print(f"  {result['total_rows']:,} rows  |  {n_ep} episode(s)  |  {result['processing_time_s']}s  |  {out_kb:.0f} KB  ->  {out_path}")
        else:
            n_trans = len(result["transitions"])
            print(f"  {result['total_rows']:,} rows  |  {n_trans} transitions  |  {result['processing_time_s']}s  |  {out_kb:.0f} KB  ->  {out_path}")


def _merge_results(results, trigger=None):
    """Merge a list of result dicts (from multiple files) into one.

    Flat mode (no trigger): unions all transitions, deduplicates by signal+time.
    Episode mode (trigger):  unions episodes; for matching trigger times (±1 s)
                             the transitions are merged; unmatched episodes are appended.
    """
    if not results:
        return {}
    if len(results) == 1:
        return results[0]

    # ── Flat mode ─────────────────────────────────────────────────────────
    if not trigger:
        seen_trans = set()
        all_trans  = []
        seen_chans = set()
        all_chans  = []
        total_rows = 0

        for r in results:
            for t in r.get("transitions", []):
                key = t["signal"] + "|" + str(t["time"])
                if key not in seen_trans:
                    seen_trans.add(key)
                    all_trans.append(t)
            for c in r.get("bool_channels", []):
                cname = c["name"] if isinstance(c, dict) else c
                if cname not in seen_chans:
                    seen_chans.add(cname)
                    all_chans.append(c)
            total_rows += r.get("total_rows", 0)

        all_trans.sort(key=lambda t: float(t["time"]))

        # Merge _plot_series across files (sort by time so episode slicing works)
        merged_plot = {}
        for r in results:
            for sig, pts in r.get("_plot_series", {}).items():
                if sig not in merged_plot:
                    merged_plot[sig] = []
                merged_plot[sig].extend(pts)
        for sig in merged_plot:
            merged_plot[sig].sort(key=lambda pt: pt[0])

        merged = dict(results[0])
        merged["transitions"]   = all_trans
        merged["bool_channels"] = all_chans
        merged["total_rows"]    = total_rows
        merged["filename"]      = f"merged ({len(results)} files)"
        if merged_plot:
            merged["_plot_series"] = merged_plot
        return merged

    # ── Episode mode ──────────────────────────────────────────────────────
    # Collect all episodes from all results; match by trigger time, merge transitions
    def trig_time_of(ep, sig):
        t = next((x for x in ep.get("transitions", [])
                  if sig.lower() in x["signal"].lower()), None)
        return float(t["time"]) if t else None

    sig = trigger
    all_episodes = []  # list of (trig_time, ep)
    for r in results:
        for ep in r.get("episodes", []):
            tt = trig_time_of(ep, sig)
            all_episodes.append((tt, ep))

    # Group by trigger time (±1 s tolerance)
    groups = []
    used   = [False] * len(all_episodes)
    for i, (tt_i, ep_i) in enumerate(all_episodes):
        if used[i]:
            continue
        group = [ep_i]
        used[i] = True
        for j, (tt_j, ep_j) in enumerate(all_episodes):
            if used[j] or i == j:
                continue
            if tt_i is not None and tt_j is not None and abs(tt_i - tt_j) < 1.0:
                group.append(ep_j)
                used[j] = True
        groups.append(group)

    merged_episodes = []
    for group in groups:
        base = dict(group[0])
        seen = set((t["signal"] + "|" + str(t["time"])) for t in base.get("transitions", []))
        merged_trans = list(base.get("transitions", []))
        merged_plots = dict(base.get("plots", {}))
        for ep in group[1:]:
            for t in ep.get("transitions", []):
                key = t["signal"] + "|" + str(t["time"])
                if key not in seen:
                    seen.add(key)
                    merged_trans.append(t)
            for sig_k, pts in ep.get("plots", {}).items():
                if sig_k not in merged_plots:
                    merged_plots[sig_k] = pts
        merged_trans.sort(key=lambda t: float(t["time"]))
        base["transitions"] = merged_trans
        base["plots"]       = merged_plots
        merged_episodes.append(base)

    merged_episodes.sort(key=lambda ep: ep.get("episode", 0))

    # Merge bool_channels
    seen_chans = set()
    all_chans  = []
    for r in results:
        for c in r.get("bool_channels", []):
            cname = c["name"] if isinstance(c, dict) else c
            if cname not in seen_chans:
                seen_chans.add(cname)
                all_chans.append(c)

    merged = dict(results[0])
    merged["episodes"]     = merged_episodes
    merged["bool_channels"] = all_chans
    merged["total_rows"]   = sum(r.get("total_rows", 0) for r in results)
    merged["filename"]     = f"merged ({len(results)} files)"
    return merged


if __name__ == "__main__":
    multiprocessing.freeze_support()   # required on Windows when frozen/spawned
    parser = argparse.ArgumentParser(description="Parallel IADS CSV analyzer")
    parser.add_argument("input",   help="Path to CSV/ZIP file, or a directory of ZIP files")
    parser.add_argument("--out",   default="analysis.json",
                        help="Output JSON path (default: analysis.json)")
    parser.add_argument("--workers", type=int, default=0,
                        help="Worker processes (default: CPU count)")
    parser.add_argument("--trigger", default=None,
                        help="Signal name that starts the window, e.g. afcsCapable")
    parser.add_argument("--trigger-from", type=float, default=1.0,
                        help="Value the trigger signal transitions FROM (default: 1)")
    parser.add_argument("--trigger-to",   type=float, default=0.0,
                        help="Value the trigger signal transitions TO   (default: 0)")
    parser.add_argument("--plot-signals", default="radAltVoted,gndSpdVoted",
                        help="Comma-separated signal names to collect for episode AGL plots "
                             "(suffix-matched, default: radAltVoted,gndSpdVoted)")
    parser.add_argument("--trace-graph", default=None,
                        help="Trace graph version expected for this sortie (embedded in output JSON)")
    parser.add_argument("--exclude-zips", default="",
                        help="Comma-separated substrings — ZIPs whose filename contains any of these are skipped")
    args = parser.parse_args()

    n = args.workers if args.workers > 0 else multiprocessing.cpu_count()
    exclude_patterns = [p.strip() for p in args.exclude_zips.split(",") if p.strip()]

    # ── Directory mode ────────────────────────────────────────────────────
    # Process every ZIP flat (no per-file trigger), merge all transitions into
    # one timeline, then run episode extraction once on the combined pool.
    # Whichever file contains the trigger signal contributes it automatically.
    if os.path.isdir(args.input):
        zip_files = sorted(glob.glob(os.path.join(args.input, "*.zip")),
                           key=os.path.getsize, reverse=True)
        if exclude_patterns:
            excluded = [z for z in zip_files if any(p in os.path.basename(z) for p in exclude_patterns)]
            zip_files = [z for z in zip_files if z not in excluded]
            if excluded:
                print(f"  Excluded {len(excluded)} ZIP(s): {', '.join(os.path.basename(z) for z in excluded)}")
        if not zip_files:
            print(f"ERROR: No ZIP files found in {args.input}")
            sys.exit(1)

        out_path = args.out
        if not os.path.isabs(out_path):
            out_path = os.path.join(os.path.abspath(args.input), out_path)

        sortie = _extract_sortie(zip_files)
        out_path = _add_sortie_suffix(out_path, sortie)

        # Pass 1: process every ZIP flat in parallel using a ThreadPool.
        # Threads safely spawn child process pools — no nested-pool issues on Windows.
        # Workers are allocated proportional to file size so large files get more cores.
        n_parallel = min(len(zip_files), max(1, n))

        # Size-proportional worker allocation: weight each file by its byte size,
        # distribute n cores proportionally, guarantee at least 1 per file.
        file_sizes  = [os.path.getsize(zf) for zf in zip_files]
        total_size  = sum(file_sizes) or 1
        raw_weights = [n * s / total_size for s in file_sizes]
        # Floor to at least 1, then redistribute the remainder to the largest files
        workers_per_file = [max(1, int(w)) for w in raw_weights]
        remainder = n - sum(workers_per_file)
        if remainder > 0:
            # Give leftover cores to the files with the largest fractional parts
            fracs = sorted(range(len(zip_files)),
                           key=lambda i: raw_weights[i] - int(raw_weights[i]),
                           reverse=True)
            for i in fracs[:remainder]:
                workers_per_file[i] += 1

        # Capture plot_signals in closure so the ThreadPool worker can access it
        _plot_sigs = args.plot_signals
        def _process_one_closure(idx_zf):
            idx, zf = idx_zf
            tmp_out = os.path.join(tempfile.gettempdir(), f"_rca_tmp_{os.getpid()}_{idx}.json")
            label   = f"[{idx + 1}/{len(zip_files)}] {os.path.basename(zf)}"
            try:
                t0 = time.perf_counter()
                process_file(zf, tmp_out, n_workers=workers_per_file[idx],
                             trigger=None,
                             plot_signals=_plot_sigs,
                             keep_plots=bool(_plot_sigs),
                             quiet=True)
                elapsed = time.perf_counter() - t0
                with open(tmp_out, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"  {label}  ({elapsed:.1f}s)")
                return data
            except Exception as e:
                print(f"  ERROR  {label}  ->  {e}")
                return None
            finally:
                if os.path.exists(tmp_out):
                    os.unlink(tmp_out)

        with ThreadPool(n_parallel) as tp:
            raw = tp.map(_process_one_closure, list(enumerate(zip_files)))

        flat_results = [r for r in raw if r is not None]

        if not flat_results:
            print("ERROR: all files failed — nothing to merge")
            sys.exit(1)

        # Pass 2: merge all flat transitions into one combined pool
        combined = _merge_results(flat_results, trigger=None)

        # Pass 3: extract episodes from the combined pool (if trigger supplied)
        if args.trigger:
            plot_series = combined.pop("_plot_series", {})
            _save_mode_transitions(combined)
            combined = _extract_episodes(combined,
                                         args.trigger,
                                         args.trigger_from,
                                         args.trigger_to)
            if plot_series:
                _attach_episode_plots(combined, plot_series)
                _save_flight_plots(combined, plot_series)
                _save_takeoff_plots(combined, plot_series)
            episodes = combined.get("episodes", [])
            if not episodes:
                print(f"  WARNING: trigger '{args.trigger}' not found in any file "
                      f"— no episodes produced")
            else:
                print(f"  {len(episodes)} episode(s)")

        if args.trace_graph:
            combined["trace_graph"] = args.trace_graph
        tail = _extract_tail(zip_files)
        if tail:
            combined["tail_number"] = tail
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(sanitize_for_json(combined), f, separators=(",", ":"), allow_nan=False)

        out_kb = os.path.getsize(out_path) / 1024
        print(f"  saved: {out_path}  ({out_kb:.0f} KB)")
        if plot_series:
            _save_hires_file(out_path, combined, plot_series)

    # ── Single file mode (existing behaviour) ────────────────────────────
    else:
        out_path = args.out
        if not os.path.isabs(out_path):
            out_path = os.path.join(os.path.dirname(os.path.abspath(args.input)), out_path)

        sortie = _extract_sortie([args.input])
        out_path = _add_sortie_suffix(out_path, sortie)
        if sortie:
            print(f"Sortie: {sortie}")

        process_file(args.input, out_path, n_workers=n,
                     trigger=args.trigger,
                     trigger_from=args.trigger_from,
                     trigger_to=args.trigger_to,
                     plot_signals=args.plot_signals,
                     trace_graph=args.trace_graph)
