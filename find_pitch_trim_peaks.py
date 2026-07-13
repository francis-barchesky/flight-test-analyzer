#!/usr/bin/env python3
"""Scan all sortie analysis JSONs for pitch-trim servo torque peaks > 25 ft-lb.

Uses the same signal matchers as the Peak Torque Histogram in
flight_test_analyzer.html (HIST_AXES.pitchTrim, line ~3207).
"""
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))

PATTERNS = [
    re.compile(r"pitchtrimservo.*trqRunningAvgVal$", re.I),
    re.compile(r"inservos.*srvPtTrqRunningAvgVal$", re.I),
    re.compile(r"Pitch_Trim_Servo\.PTSRV.*Torque_Running_Avg\.?$", re.I),
]

THRESHOLD = float(sys.argv[1]) if len(sys.argv) > 1 else 25.0


def main():
    results = []
    scanned = 0
    with_data = 0
    for d in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, d)
        if not os.path.isdir(p):
            continue
        main_json = None
        for f in os.listdir(p):
            if f.startswith("analysis") and f.endswith(".json") and "_hires" not in f:
                main_json = os.path.join(p, f)
                break
        if not main_json:
            continue
        scanned += 1
        try:
            with open(main_json, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        ts = data.get("torque_stats") or []
        peak = 0.0
        sig_hit = None
        peak_time = None
        for s in ts:
            sig = s.get("signal", "")
            pk = s.get("peak")
            if pk is None:
                continue
            if any(rx.search(sig) for rx in PATTERNS):
                v = abs(pk)
                if v > peak:
                    peak = v
                    sig_hit = sig
                    peak_time = s.get("peak_time")
        if sig_hit is not None:
            with_data += 1
        if peak >= THRESHOLD:
            results.append((d, peak, peak_time, sig_hit))

    results.sort(key=lambda r: -r[1])
    print(f"Scanned {scanned} sortie dirs; {with_data} have a pitch-trim torque signal.")
    print(f"{len(results)} sortie(s) with pitch-trim servo peak >= {THRESHOLD} ft-lb:")
    print()
    print(f"{'Sortie':<22} {'Peak':>8}  {'Peak time':>11}   Signal")
    print("-" * 110)
    for name, peak, t, sig in results:
        ts = f"{t:.2f}" if isinstance(t, (int, float)) else "-"
        print(f"{name:<22} {peak:>8.2f}  {ts:>11}   {sig}")


if __name__ == "__main__":
    main()
