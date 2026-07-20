"""
extract_afcs.py — extract all signals needed by estimate_inertia.py
from raw ZIP files at full source rate (100 Hz) and cache to .afcs_signals.npz.

Signals come from:
  AFCS_del5_*.zip          — surfaces, body rates, accel, TAS, weight/CG
  iads_pressure_altitude_* — pressure altitude

Run once per sortie dataset before estimate_inertia.py.

Usage:
    python extract_afcs.py                   # all N208B sorties with ZIPs
    python extract_afcs.py --sortie S143_1   # single sortie (debug)
    python extract_afcs.py --workers 6       # parallel
    python extract_afcs.py --force           # ignore existing cache
"""
import argparse
import csv
import glob
import hashlib
import io
import os
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np

ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = ".afcs_signals.npz"
SCHEMA_VERSION = 2   # bumped: added body rates, tas, press_alt

# Column names as they appear in the ZIPs.
# Key = logical name used by estimate_inertia.py.
AFCS_SIGNAL_MAP = {
    # ── Surface positions (AFCS_del5) ─────────────────────────────────────────
    "daL":      "Left_Aileron_lvdt2deg",
    "daR":      "Right_Aileron_lvdt2deg",
    "dr":       "Rudder_lvdt2deg",
    "de":       "Elevator_lvdt2deg",
    # ── Fuel / weight (AFCS_del5) ─────────────────────────────────────────────
    "mL":       "lt_fuel_wt",
    "mR":       "rt_fuel_wt",
    "fuel":     "total_fuel",
    "wt":       "WT_current",
    "cg":       "CG_current",
    # ── Body rates  deg/s  (AFCS_del5 — FCC1A voted values) ──────────────────
    "p":        "FCC1A.g_bodyrollrate_mdlrefdw.rtb.bodyRollRateVotedValue",
    "q":        "FCC1A.g_bodypitchrate_mdlrefdw.rtb.bodyPitchRateVotedValue",
    "r":        "FCC1A.g_bodyyawrate_mdlrefdw.rtb.bodyYawRateVotedValue",
    # ── Accelerations  ft/s²  (AFCS_del5) ─────────────────────────────────────
    "ay":       "FCC1A.g_bodylataccel_mdlrefdw.rtb.bodyLatAccelVotedValue",
    # ── Airspeed  ft/s  (AFCS_del5) ───────────────────────────────────────────
    "tas":      "FCC1A.g_sensorfilters_mdlrefdw.rtb.tasVoted",
    # ── Pressure altitude  ft  (iads_pressure_altitude ZIP) ───────────────────
    "press_alt": "FCC1A.g_pressurealtitude_mdlrefdw.rtb.votedValue",
}


def _parse_time(s):
    parts = s.split(":")
    if len(parts) == 4:
        d, h, m, sec = parts
        return int(d) * 86400 + int(h) * 3600 + int(m) * 60 + float(sec)
    if len(parts) == 3:
        h, m, sec = parts
        return int(h) * 3600 + int(m) * 60 + float(sec)
    return float(s)


def _extract_zip(zip_path, wanted_cols):
    """Read named columns from the first CSV in the ZIP. Returns {col_name: (t, y)}."""
    result = {}
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                return result
            raw = zf.open(csvs[0])
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
            header = [h.strip().rstrip(".") for h in next(reader)]
            time_col = None
            for i, h in enumerate(header):
                if h.lower().startswith("time") or h.lower() == "irig":
                    time_col = i
                    break
            if time_col is None:
                return result
            col_idx = {}
            for name in wanted_cols:
                try:
                    col_idx[name] = header.index(name)
                except ValueError:
                    pass
            if not col_idx:
                return result
            buckets = {n: [] for n in col_idx}
            for row in reader:
                if not row or len(row) <= time_col:
                    continue
                try:
                    t = _parse_time(row[time_col])
                except Exception:
                    continue
                for name, j in col_idx.items():
                    if j >= len(row):
                        continue
                    v = row[j]
                    if not v or v.lower() == "nan":
                        continue
                    try:
                        buckets[name].append((t, float(v)))
                    except ValueError:
                        pass
    except Exception:
        return result

    for name, pts in buckets.items():
        if len(pts) < 2:
            continue
        arr = np.asarray(pts, dtype=float)
        order = np.argsort(arr[:, 0], kind="stable")
        arr = arr[order]
        keep = np.concatenate(([True], np.diff(arr[:, 0]) > 0))
        result[name] = (arr[keep, 0], arr[keep, 1])
    return result


def _cache_key(zip_paths):
    parts = [f"schema:v{SCHEMA_VERSION}"]
    for p in sorted(zip_paths):
        st = os.stat(p)
        parts.append(f"{os.path.basename(p)}:{st.st_mtime_ns}:{st.st_size}")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _load_cache(cache_path, key):
    if not os.path.exists(cache_path):
        return None
    try:
        npz = np.load(cache_path, allow_pickle=False)
        if str(npz["_key"]) != key:
            return None
        names = [n for n in str(npz["_names"]).split("|") if n]
        return {n: (npz[f"{n}__t"], npz[f"{n}__y"]) for n in names}
    except Exception:
        return None


def _save_cache(cache_path, key, found):
    names = list(found.keys())
    payload = {
        "_key":   np.array(key, dtype="<U64"),
        "_names": np.array("|".join(names), dtype="<U4096"),
    }
    for name, (t, y) in found.items():
        payload[f"{name}__t"] = t
        payload[f"{name}__y"] = y
    np.savez_compressed(cache_path, **payload)


def process_sortie(sortie_dir, force=False, verbose=False):
    """Extract AFCS signals for one sortie. Returns (n_found, status_msg)."""
    zips = sorted(glob.glob(os.path.join(sortie_dir, "*.zip")))
    if not zips:
        return 0, "no ZIPs"

    cache_path = os.path.join(sortie_dir, CACHE_FILE)
    key = _cache_key(zips)

    if not force:
        cached = _load_cache(cache_path, key)
        if cached is not None:
            return len(cached), "cache hit"

    wanted = list(AFCS_SIGNAL_MAP.values())
    found_by_col = {}

    for zp in zips:
        remaining = [v for v in wanted if v not in found_by_col]
        if not remaining:
            break
        data = _extract_zip(zp, remaining)
        if verbose and data:
            for col, (t, y) in data.items():
                dt = float(np.median(np.diff(t))) if len(t) > 1 else float("nan")
                hz = 1.0 / dt if dt > 0 else float("nan")
                valid = int(np.sum(np.isfinite(y)))
                print(f"    {os.path.basename(zp)}: {col} — {len(t)} pts @ {hz:.0f} Hz, {valid} finite")
        found_by_col.update(data)

    found = {}
    for logical, col in AFCS_SIGNAL_MAP.items():
        if col in found_by_col:
            found[logical] = found_by_col[col]

    if not found:
        return 0, "no target signals in ZIPs"

    _save_cache(cache_path, key, found)
    return len(found), f"extracted {len(found)}/{len(AFCS_SIGNAL_MAP)}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sortie",  default=None, help="Limit to one sortie substring")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--force",   action="store_true", help="Rebuild even if cache exists")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dirs = sorted(glob.glob(os.path.join(ROOT, "*_N208B")))
    if args.sortie:
        dirs = [d for d in dirs if args.sortie in os.path.basename(d)]
    dirs = [d for d in dirs if os.path.isdir(d)
            and glob.glob(os.path.join(d, "*.zip"))]
    print(f"Processing {len(dirs)} sortie(s) with ZIPs...")

    def _work(d):
        name = os.path.basename(d)
        n, msg = process_sortie(d, force=args.force, verbose=args.verbose)
        print(f"  {name:30s}  {msg}", flush=True)
        return n

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(ex.map(_work, dirs))
    else:
        for d in dirs:
            _work(d)

    print("Done.")


if __name__ == "__main__":
    main()
