"""
inject_dgps.py — inject DGPS signals into an analysis_*_hires.json.

Reads the hires JSON, looks up each plot's time range, slices the DGPS
stream to that window, and appends four new signals:
  dgps_lat_dd, dgps_lon_dd, dgps_alt_msl_m, dgps_hae_m

Times in the hires JSON are absolute seconds: day * 86400 + UTC-seconds-since-midnight,
where day is the IADS day-of-year (0-based).  Day 110 = 2026-04-21.

Usage:
  python inject_dgps.py <hires_json> [dgps_stream50_csv]

The file is updated in-place (original backed up as *_pre_dgps.json).
"""

import csv
import json
import bisect
import math
import sys
from pathlib import Path

BASE_DAY       = 110              # IADS day-of-year for 2026-04-21
BASE_S         = BASE_DAY * 86_400   # 9,504,000 s  (absolute seconds at midnight Apr 21)
DGPS_SIGNALS   = ["dgps_lat_dd", "dgps_lon_dd", "dgps_alt_msl_m", "dgps_hae_m"]
DGPS_SRC_COLS  = ["lat_dd",      "lon_dd",      "altitude_msl_m", "hae_m"]
HIRES_MAX_PTS  = 8_000

DGPS_DEFAULT = Path(__file__).parent.parent / "data" / "S022ZKMLN" / "DGPS" / \
               "TrimbleDataRaw_2026-04-21_stream50.csv"


def dgps_utc_to_abs(token: str) -> float:
    """'HH:MM:SS.S' → absolute seconds (day 110 base, midnight rollover → day 111)."""
    h, m, s = token.strip().split(":")
    hi = int(h)
    t = hi * 3600 + int(m) * 60 + float(s)
    day = BASE_DAY + (1 if hi < 12 else 0)   # past midnight → next day
    return day * 86_400 + t - hi * 3600 + hi * 3600   # = day*86400 + t


def load_dgps(path: Path) -> tuple[list[float], dict[str, list[float]]]:
    """Return (sorted abs-times, {col: [values]}) for the DGPS stream."""
    times = []
    cols  = {c: [] for c in DGPS_SRC_COLS}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                t = dgps_utc_to_abs(row["time_utc"])
            except Exception:
                continue
            times.append(t)
            for c in DGPS_SRC_COLS:
                try:
                    v = float(row[c])
                    cols[c].append(v if math.isfinite(v) else None)
                except (ValueError, TypeError):
                    cols[c].append(None)
    return times, cols


def minmax_downsample(pts: list, max_pts: int) -> list:
    """Min-max bucket downsample matching analyze_iads.py's _downsample_pts."""
    if len(pts) <= max_pts:
        return pts
    bucket_count = max(1, max_pts // 2)
    step = len(pts) / bucket_count
    out = []
    for i in range(bucket_count):
        lo, hi = int(i * step), int((i + 1) * step)
        bucket = pts[lo:hi]
        if not bucket:
            continue
        mn = min(bucket, key=lambda p: p[1])
        mx = max(bucket, key=lambda p: p[1])
        if mn[0] <= mx[0]:
            out.append(mn); (out.append(mx) if mn is not mx else None)
        else:
            out.append(mx); (out.append(mn) if mn is not mx else None)
    return out


def inject_into_section(section: dict, dgps_times: list[float],
                         dgps_cols: dict[str, list[float]]) -> int:
    """Add DGPS signals to one plot section dict.  Returns count of points added."""
    # Determine time window from existing signals
    t_lo = math.inf
    t_hi = -math.inf
    for pts in section.values():
        if pts:
            t_lo = min(t_lo, pts[0][0])
            t_hi = max(t_hi, pts[-1][0])

    if not math.isfinite(t_lo):
        return 0

    lo_idx = bisect.bisect_left(dgps_times,  t_lo)
    hi_idx = bisect.bisect_right(dgps_times, t_hi)

    for sig, src in zip(DGPS_SIGNALS, DGPS_SRC_COLS):
        raw = [
            [dgps_times[i], dgps_cols[src][i]]
            for i in range(lo_idx, hi_idx)
            if dgps_cols[src][i] is not None
        ]
        pts = minmax_downsample(raw, HIRES_MAX_PTS)
        section[sig] = pts

    return hi_idx - lo_idx


def main():
    hires_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    dgps_path  = Path(sys.argv[2]) if len(sys.argv) > 2 else DGPS_DEFAULT

    if hires_path is None:
        print("Usage: python inject_dgps.py <hires_json> [dgps_stream50_csv]")
        sys.exit(1)

    if not hires_path.exists():
        print(f"ERROR: {hires_path} not found")
        sys.exit(1)
    if not dgps_path.exists():
        print(f"ERROR: DGPS file not found: {dgps_path}")
        sys.exit(1)

    print(f"Loading DGPS: {dgps_path}")
    dgps_times, dgps_cols = load_dgps(dgps_path)
    print(f"  {len(dgps_times)} fixes  abs-t [{dgps_times[0]:.1f} - {dgps_times[-1]:.1f}]")

    print(f"Loading hires JSON: {hires_path}")
    with open(hires_path, encoding="utf-8") as f:
        data = json.load(f)

    # Backup
    backup = hires_path.with_name(hires_path.stem + "_pre_dgps.json")
    backup.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    print(f"  Backup: {backup}")

    for section_key in ("flight_plots", "takeoff_plots"):
        section = data.get(section_key)
        if section is None:
            print(f"  {section_key}: missing, skipping")
            continue
        n = inject_into_section(section, dgps_times, dgps_cols)
        pts_added = sum(len(section[s]) for s in DGPS_SIGNALS if s in section)
        print(f"  {section_key}: {n} DGPS fixes in window, {pts_added} pts added across {len(DGPS_SIGNALS)} signals")

    with open(hires_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), allow_nan=False)

    size_kb = hires_path.stat().st_size / 1024
    print(f"Done. {hires_path}  ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
