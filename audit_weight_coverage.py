"""
audit_weight_coverage.py — scan all analysis_*.json files and report
which sorties have weight datagroup signals populated. Pulls min/max
from num_channels (which is always populated), not just plot series.
"""
import json
import os
import glob
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))

WEIGHT_SIGS = [
    "lt_fuel_wt", "rt_fuel_wt", "total_fuel",
    "ZFW_updated", "ZFWCG_updated",
    "CG_current", "WT_current",
]


def sig_key(name):
    return name.rstrip(".").split(".")[-1]


def find_num_channel(num_channels, sig):
    """Look up a signal in num_channels by exact name or last-dot suffix."""
    for c in num_channels:
        n = c.get("name", "")
        if n == sig or sig_key(n) == sig:
            return c
    return None


def audit_sortie(analysis_path):
    try:
        with open(analysis_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"sortie": os.path.basename(analysis_path), "error": str(e)}

    headers = data.get("headers", [])
    header_set = {sig_key(h) for h in headers}
    weight_hits = [s for s in WEIGHT_SIGS if s in header_set]

    num_channels = data.get("num_channels", [])
    ranges = {}
    for sig in WEIGHT_SIGS:
        c = find_num_channel(num_channels, sig)
        if c is not None:
            ranges[sig] = (c.get("min"), c.get("max"), c.get("mean"))

    return {
        "sortie":       os.path.basename(os.path.dirname(analysis_path)),
        "generated_at": data.get("generated_at", ""),
        "n_rows":       data.get("total_rows", 0),
        "n_episodes":   len(data.get("episodes", []) or []),
        "duration_s":   data.get("duration_s"),
        "weight_hits":  weight_hits,
        "ranges":       ranges,
    }


def main():
    files = sorted(glob.glob(os.path.join(ROOT, "*_N208B", "analysis_*.json")))
    files = [f for f in files if not f.endswith("_hires.json")]

    with_ranges = []
    with_header_only = []
    without = []

    for path in files:
        r = audit_sortie(path)
        if r.get("ranges"):
            with_ranges.append(r)
        elif r.get("weight_hits"):
            with_header_only.append(r)
        else:
            without.append(r)

    print(f"Total analysis files:                    {len(files)}")
    print(f"With numeric ranges for weight signals:  {len(with_ranges)}")
    print(f"With weight in headers but no ranges:    {len(with_header_only)}")
    print(f"Without any weight signals:              {len(without)}")
    print()

    if not with_ranges:
        print("No sorties have numeric ranges — nothing to summarize.")
        return

    print("--- Sorties with weight signal ranges (from num_channels) ---")
    print(f"{'sortie':<18} {'gen_at':<20} {'eps':>3} "
          f"{'total_fuel_lb':>26} {'WT_current_lb':>26} {'CG_current':>22}")
    for r in sorted(with_ranges, key=lambda x: x["sortie"]):
        gen = (r["generated_at"] or "")[:19]
        def fmt(sig):
            v = r["ranges"].get(sig)
            if v is None or v[0] is None or v[1] is None:
                return "n/a".rjust(26)
            return f"{v[0]:>10.1f}..{v[1]:<10.1f}"[:26].rjust(26)
        print(f"{r['sortie']:<18} {gen:<20} {r['n_episodes']:>3} "
              f"{fmt('total_fuel')} {fmt('WT_current')} {fmt('CG_current')}")

    print()
    print("--- Aggregate spread across sorties WITH ranges ---")
    for k in ("total_fuel", "WT_current", "CG_current", "lt_fuel_wt", "rt_fuel_wt"):
        vals_min = []
        vals_max = []
        for r in with_ranges:
            v = r["ranges"].get(k)
            if v is None: continue
            if v[0] is not None: vals_min.append(v[0])
            if v[1] is not None: vals_max.append(v[1])
        if vals_min and vals_max:
            print(f"  {k:<15} n_sorties={len(vals_min):>3}  "
                  f"min-of-mins={min(vals_min):>8.2f}  "
                  f"max-of-maxes={max(vals_max):>8.2f}  "
                  f"span={max(vals_max)-min(vals_min):>8.2f}")

    # Episode count summary — how many sorties have roll excitation available?
    ep_summary = {}
    for r in with_ranges:
        n = r["n_episodes"]
        bucket = "0" if n == 0 else ("1-3" if n <= 3 else ("4-9" if n <= 9 else "10+"))
        ep_summary[bucket] = ep_summary.get(bucket, 0) + 1
    print()
    print("--- Episode count histogram (sorties WITH weight ranges) ---")
    for b in ("0", "1-3", "4-9", "10+"):
        print(f"  {b:<5} episodes:  {ep_summary.get(b, 0)}")


if __name__ == "__main__":
    main()
