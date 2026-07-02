#!/usr/bin/env python3
"""Diagnostic for pitch-trim slip clutch: bin the actual/estimated tracking ratio
by instantaneous |pinion torque| across S118_1/2/3 (or any sorties with capstan
position).

Torque units: PTSRV Torque_Running_Avg is % of 25 in-lb at the PINION.
   pinion_inlb = (torque_pct / 100) * 25

If the servo's plate friction clutch is slipping under torque, the tracking
ratio should DROP as |torque| rises. A flat ratio vs torque would rule out
clutch slippage and point to a fixed scale error (motor speed, gear
tolerances) instead.

Method:
  1. Compute instantaneous rate of actual position (deg/s) and estimated
     position rate from cmd (deg/s).
  2. Compute |PTSRV pinion torque| in %.
  3. Only keep samples where |estimated rate| > threshold (so we're not
     dividing near-zero by near-zero during quiescent periods).
  4. Fit k = actual/est (through origin) within each torque bin.
  5. Report k vs torque bin per sortie.

Usage:
    py313 pitch_trim_clutch_check.py                # +/-30 s window, 20 %-bins
    py313 pitch_trim_clutch_check.py 60 10          # wider window, finer bins
"""
import glob
import json
import math
import os
import re
import sys

import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))

MAX_PINION_RPM          = 440.0
GEAR_RATIO              = 8.995
CAPSTAN_TO_SENSOR_RATIO = 2.0645
# PTSRV Torque_Running_Avg is % of 25 in-lb at the pinion
PINION_TORQUE_MAX_INLB  = 25.0
SENSOR_DPS_PER_PCT = (MAX_PINION_RPM / GEAR_RATIO
                     / CAPSTAN_TO_SENSOR_RATIO
                     * 360.0 / 60.0 / 100.0)

PTRIM_PATTERNS = [
    re.compile(r"Pitch_Trim_Servo\.PTSRV.*Torque_Running_Avg\.?$", re.I),
]
CMD_RE = re.compile(r"aptTrimRateServoCmd$", re.I)
POS_RE = re.compile(r"Elev_Trim_Tab_Capstan_Pos\.?$", re.I)


def merged_plots(hires):
    out = {}
    for k in ("flight_plots", "takeoff_plots", "full_flight_torque"):
        for sig, pts in (hires.get(k) or {}).items():
            out.setdefault(sig, []).extend(pts)
    for sig in out:
        out[sig].sort(key=lambda p: p[0])
    return out


def find_peak_time(plots):
    best_t, best_v = None, 0.0
    for sig, pts in plots.items():
        if not any(rx.search(sig) for rx in PTRIM_PATTERNS):
            continue
        for t, v in pts:
            if v is not None and abs(v) > abs(best_v):
                best_t, best_v = t, v
    return best_t, best_v


def interp(ts, vs, targets):
    if not ts:
        return [float("nan")] * len(targets)
    out = []
    j = 0
    for t in targets:
        while j + 1 < len(ts) and ts[j + 1] < t:
            j += 1
        if t < ts[0]:
            out.append(vs[0])
        elif j + 1 >= len(ts) or ts[j] > t:
            out.append(vs[j])
        else:
            t0, t1 = ts[j], ts[j + 1]
            v0, v1 = vs[j], vs[j + 1]
            frac = (t - t0) / (t1 - t0) if t1 > t0 else 0.0
            out.append(v0 + frac * (v1 - v0))
    return out


def process_sortie(sortie_dir, window_s, min_est_rate_dps):
    hires_files = glob.glob(os.path.join(sortie_dir, "*_hires.json"))
    if not hires_files:
        return None
    with open(hires_files[0], encoding="utf-8") as f:
        h = json.load(f)
    plots = merged_plots(h)

    cmd = [(s, pts) for s, pts in plots.items() if CMD_RE.search(s) and pts]
    pos = [(s, pts) for s, pts in plots.items() if POS_RE.search(s) and pts]
    torque_series = [(s, pts) for s, pts in plots.items()
                     if any(rx.search(s) for rx in PTRIM_PATTERNS) and pts]
    if not (cmd and pos and torque_series):
        return None

    t_peak, v_peak = find_peak_time(plots)
    if t_peak is None:
        return None
    t_lo, t_hi = t_peak - window_s, t_peak + window_s

    # Windowed cmd (%)
    cmd_seg = [p for p in cmd[0][1] if t_lo <= p[0] <= t_hi]
    if len(cmd_seg) < 4:
        return None
    cmd_t = [p[0] for p in cmd_seg]
    cmd_v = [p[1] for p in cmd_seg]

    # Estimated rate (deg/s at sensor) from cmd — the cmd itself scaled
    est_rate = [c * SENSOR_DPS_PER_PCT for c in cmd_v]

    # Actual position derivative — use RDC1 (lane 0). Compute rate at cmd
    # timestamps by central-difference on the interpolated position curve.
    pos_lane = pos[0]
    pos_t = [p[0] for p in pos_lane[1] if t_lo - 1 <= p[0] <= t_hi + 1]
    pos_v = [p[1] for p in pos_lane[1] if t_lo - 1 <= p[0] <= t_hi + 1]
    if len(pos_t) < 4:
        return None

    # Sample position at cmd timestamps offset by +/- 0.25 s for central difference
    dt = 0.25
    pos_plus  = interp(pos_t, pos_v, [t + dt for t in cmd_t])
    pos_minus = interp(pos_t, pos_v, [t - dt for t in cmd_t])
    act_rate = [(p - m) / (2 * dt) for p, m in zip(pos_plus, pos_minus)]

    # |Torque| interpolated at cmd timestamps: max of TX_1/TX_2 |v|
    tq_at_cmd = [0.0] * len(cmd_t)
    for _sig, pts in torque_series:
        seg = [p for p in pts if t_lo - 1 <= p[0] <= t_hi + 1]
        if not seg:
            continue
        ts, vs = zip(*seg)
        v_at = interp(list(ts), list(vs), cmd_t)
        for i, v in enumerate(v_at):
            tq_at_cmd[i] = max(tq_at_cmd[i], abs(v))

    # Return per-sample records; we'll filter and bin at the aggregate stage
    records = []
    for i in range(len(cmd_t)):
        if abs(est_rate[i]) < min_est_rate_dps:
            continue
        records.append({
            "t": cmd_t[i], "cmd": cmd_v[i],
            "est_rate": est_rate[i], "act_rate": act_rate[i],
            "abs_tq": tq_at_cmd[i],
        })
    return {"name": os.path.basename(sortie_dir),
            "peak_v": v_peak, "records": records}


def fit_k_through_origin(xs, ys):
    num = sum(x * y for x, y in zip(xs, ys))
    den = sum(x * x for x in xs)
    if den < 1e-12:
        return float("nan"), float("nan"), 0
    k = num / den
    residuals = [y - k * x for x, y in zip(xs, ys)]
    rms = math.sqrt(sum(r * r for r in residuals) / len(residuals))
    return k, rms, len(xs)


def main():
    window_s      = float(sys.argv[1]) if len(sys.argv) > 1 else 30.0
    bin_pct       = float(sys.argv[2]) if len(sys.argv) > 2 else 20.0
    min_est_dps   = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0

    print(f"Sensor deg/s per %cmd = {SENSOR_DPS_PER_PCT:.4f}   "
          f"(gear {GEAR_RATIO}, sensor {CAPSTAN_TO_SENSOR_RATIO})")
    print(f"Torque bin: {bin_pct}% of {PINION_TORQUE_MAX_INLB} in-lb pinion  "
          f"= {bin_pct*PINION_TORQUE_MAX_INLB/100:.2f} in-lb per bin")
    print(f"Window +/-{window_s:.0f}s around peak  |  "
          f"|est_rate| > {min_est_dps} deg/s")
    print()

    sorties = []
    for d in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, d)
        if not os.path.isdir(p):
            continue
        r = process_sortie(p, window_s, min_est_dps)
        if r and r["records"]:
            sorties.append(r)

    if not sorties:
        print("No sorties with capstan position + cmd in window.")
        return

    # Combined record set to bin globally
    all_recs = []
    for s in sorties:
        for r in s["records"]:
            all_recs.append(r)
    print(f"Total samples: {len(all_recs)} across {len(sorties)} sortie(s)")

    # Determine bins from 0 up to max torque, in bin_ftlb-sized bins
    max_tq = max(r["abs_tq"] for r in all_recs)
    n_bins = int(math.ceil(max_tq / bin_pct))
    if n_bins < 1:
        n_bins = 1
    edges = [i * bin_pct for i in range(n_bins + 1)]

    # Per-sortie fit per bin, then a combined fit
    print("\nPer-sortie |k| by torque bin (fit act_rate = k * est_rate; sign-negated for readability):")
    print(f"{'Sortie':<16} " + "  ".join(f"[{edges[i]:>4.1f}-{edges[i+1]:>4.1f}]"
                                          for i in range(n_bins))
          + "   overall")
    for s in sorties:
        row = [s["name"]]
        bin_ks = []
        for i in range(n_bins):
            lo, hi = edges[i], edges[i + 1]
            xs = [r["est_rate"] for r in s["records"]
                  if lo <= r["abs_tq"] < hi]
            ys = [r["act_rate"] for r in s["records"]
                  if lo <= r["abs_tq"] < hi]
            k, _, n = fit_k_through_origin(xs, ys)
            bin_ks.append((k, n))
            if n < 5:
                row.append("   -   ")
            else:
                row.append(f" {abs(k):>5.3f}({n:>4})")
        xs = [r["est_rate"] for r in s["records"]]
        ys = [r["act_rate"] for r in s["records"]]
        k, rms, n = fit_k_through_origin(xs, ys)
        row.append(f" {abs(k):>5.3f}({n:>4})")
        print(" ".join(row))

    # Combined
    print("\nCombined fit across all sorties per torque bin:")
    print(f"{'Bin (%pinion)':<15} {'in-lb':>10}  {'|k|':>8} {'n':>6} {'RMS(deg/s)':>12}")
    bin_summary = []
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        xs = [r["est_rate"] for r in all_recs if lo <= r["abs_tq"] < hi]
        ys = [r["act_rate"] for r in all_recs if lo <= r["abs_tq"] < hi]
        k, rms, n = fit_k_through_origin(xs, ys)
        if n >= 5:
            mid = 0.5 * (lo + hi)
            inlb = mid * PINION_TORQUE_MAX_INLB / 100.0
            print(f"[{lo:>5.1f},{hi:>5.1f}]  {inlb:>10.3f}  {abs(k):>8.4f} "
                  f"{n:>6d} {rms:>12.3f}")
            bin_summary.append((mid, abs(k), n, rms))

    # Plot |k| vs torque
    if bin_summary:
        xs = [b[0] for b in bin_summary]
        ks = [b[1] for b in bin_summary]
        ns = [b[2] for b in bin_summary]
        fig, ax = plt.subplots(figsize=(9, 4.5))
        ax.plot(xs, ks, "o-", color="#5E3D8C", lw=1.4, ms=7)
        ax.axhline(1.0, color="#4CC9A0", lw=0.6, ls="--", alpha=0.7,
                   label="ideal (no slip)")
        ax.axhline(0.940, color="#E08040", lw=0.6, ls=":", alpha=0.8,
                   label="overall mean 0.940")
        for x, k, n in zip(xs, ks, ns):
            ax.annotate(f"n={n}", xy=(x, k), xytext=(0, 8),
                        textcoords="offset points", ha="center",
                        fontsize=8, color="#78766e")
        ax.set_xlabel(f"|PTSRV pinion torque| bin center  "
                      f"(% of {PINION_TORQUE_MAX_INLB:.0f} in-lb)")
        ax.set_ylabel("|k| = actual rate / estimated rate")
        ax.set_title(f"Pitch-trim tracking ratio vs instantaneous |pinion torque|  "
                     f"(S118_1/2/3 combined)  "
                     f"[100% = {PINION_TORQUE_MAX_INLB} in-lb pinion]")
        # Add secondary in-lb axis on top
        ax_top = ax.twiny()
        xlo, xhi = ax.get_xlim()
        ax_top.set_xlim(xlo * PINION_TORQUE_MAX_INLB / 100,
                        xhi * PINION_TORQUE_MAX_INLB / 100)
        ax_top.set_xlabel("pinion torque (in-lb)")
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=9)
        out = os.path.join(ROOT, "pitch_trim_clutch_check.png")
        fig.tight_layout()
        fig.savefig(out, dpi=150)
        print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
