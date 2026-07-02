#!/usr/bin/env python3
"""Compute the actual/estimated fit ratio for pitch-trim capstan position across
sorties that have Elev_Trim_Tab_Capstan_Pos in their hires JSON.

For each sortie:
  - Find the pitch-trim torque peak time
  - Integrate aptTrimRateServoCmd through motor(440 RPM)/gearbox(8.995)/sensor-gear(2.0645)
    to get estimated sensor-degrees vs time
  - Sample estimated at each actual position sample, fit k = actual/est through origin
  - Report per-sortie k, RMS residual, and a fit using only points within +/- window_s
    of the peak (where cmd amplitude is largest and fit is best-constrained)

Usage:
    py313 pitch_trim_fit_scan.py                  # +/-30 s window around peak
    py313 pitch_trim_fit_scan.py 60               # wider window
"""
import glob
import json
import math
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))

MAX_PINION_RPM          = 440.0
GEAR_RATIO              = 8.995
CAPSTAN_TO_SENSOR_RATIO = 2.0645
SENSOR_DPS_PER_PCT = (MAX_PINION_RPM / GEAR_RATIO
                     / CAPSTAN_TO_SENSOR_RATIO
                     * 360.0 / 60.0 / 100.0)

# ── Kaney ICD D-22999 Rev. B pg 41 (Reduced Requirement, black curve) ────────
ICD_TORQUE_SPEED = [
    (  0.0, 440.0), ( 40.0, 440.0), ( 50.0, 410.0), ( 60.0, 400.0),
    ( 70.0, 380.0), ( 80.0, 350.0), ( 85.0, 300.0),
    ( 90.0, 250.0), ( 95.0, 100.0), (100.0,   0.0), (200.0,   0.0),
]


def motor_max_rpm(torque_pct, curve=ICD_TORQUE_SPEED):
    t = abs(torque_pct)
    if t <= curve[0][0]:
        return curve[0][1]
    if t >= curve[-1][0]:
        return curve[-1][1]
    for i in range(1, len(curve)):
        t0, r0 = curve[i - 1]
        t1, r1 = curve[i]
        if t <= t1:
            frac = (t - t0) / (t1 - t0) if t1 > t0 else 0.0
            return r0 + frac * (r1 - r0)
    return curve[-1][1]

PTRIM_PATTERNS = [
    re.compile(r"pitchtrimservo.*trqRunningAvgVal$", re.I),
    re.compile(r"inservos.*srvPtTrqRunningAvgVal$", re.I),
    re.compile(r"Pitch_Trim_Servo\.PTSRV.*Torque_Running_Avg\.?$", re.I),
]
TQ_LANE_PATTERNS = [
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


def find_peak(plots):
    best = (None, 0.0, None)
    for sig, pts in plots.items():
        if not any(rx.search(sig) for rx in PTRIM_PATTERNS):
            continue
        for t, v in pts:
            if v is None:
                continue
            if abs(v) > abs(best[1]):
                best = (t, v, sig)
    return best


def integrate_cmd(cmd_pts, t_lo, t_hi, tq_at_cmd=None):
    """Integrate rate cmd → sensor degrees. If tq_at_cmd (aligned to cmd_pts) is
    provided, apply the ICD torque-speed curve to cap motor RPM at each step."""
    seg = [p for p in cmd_pts if t_lo <= p[0] <= t_hi]
    if not seg:
        return [], []
    ts, vs = zip(*seg)
    ts, vs = list(ts), list(vs)
    acc = 0.0
    out_t, out_deg = [ts[0]], [0.0]
    for i in range(1, len(ts)):
        dt = ts[i] - ts[i - 1]
        cmd_avg = 0.5 * (vs[i] + vs[i - 1])
        if tq_at_cmd is not None:
            tq_avg = 0.5 * (tq_at_cmd[i] + tq_at_cmd[i - 1])
            cmd_rpm = cmd_avg / 100.0 * MAX_PINION_RPM
            cap_rpm = motor_max_rpm(tq_avg)
            if abs(cmd_rpm) > cap_rpm:
                actual_rpm = math.copysign(cap_rpm, cmd_rpm)
            else:
                actual_rpm = cmd_rpm
            sensor_dps = (actual_rpm * 360.0 / 60.0
                          / GEAR_RATIO / CAPSTAN_TO_SENSOR_RATIO)
            acc += sensor_dps * dt
        else:
            acc += cmd_avg * SENSOR_DPS_PER_PCT * dt
        out_t.append(ts[i])
        out_deg.append(acc)
    return out_t, out_deg


def interp_series(ts, vs, targets):
    if not ts:
        return [0.0] * len(targets)
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


def analyze_sortie(sortie_dir, window_s):
    hires_files = glob.glob(os.path.join(sortie_dir, "*_hires.json"))
    if not hires_files:
        return None
    with open(hires_files[0], encoding="utf-8") as f:
        h = json.load(f)
    plots = merged_plots(h)

    pos = [(s, pts) for s, pts in plots.items() if POS_RE.search(s) and pts]
    if not pos:
        return None
    cmd = [(s, pts) for s, pts in plots.items() if CMD_RE.search(s) and pts]
    if not cmd:
        return None
    t_peak, v_peak, sig_peak = find_peak(plots)
    if t_peak is None:
        return None

    t_lo, t_hi = t_peak - window_s, t_peak + window_s

    # Build |torque| aligned to cmd timestamps (max of TX_1/TX_2)
    cmd_seg = [p for p in cmd[0][1] if t_lo <= p[0] <= t_hi]
    cmd_ts = [p[0] for p in cmd_seg]
    tq_at_cmd = [0.0] * len(cmd_ts)
    for _s, pts in plots.items():
        if not any(rx.search(_s) for rx in TQ_LANE_PATTERNS):
            continue
        seg = [p for p in pts if t_lo - 1 <= p[0] <= t_hi + 1]
        if not seg:
            continue
        ts_t = [p[0] for p in seg]
        vs_t = [abs(p[1]) for p in seg]
        v_at = interp_series(ts_t, vs_t, cmd_ts)
        for i, v in enumerate(v_at):
            tq_at_cmd[i] = max(tq_at_cmd[i], v)

    # Naive (no curve) and ICD-corrected estimates
    est_t_naive, est_deg_naive = integrate_cmd(cmd[0][1], t_lo, t_hi, tq_at_cmd=None)
    est_t,       est_deg       = integrate_cmd(cmd[0][1], t_lo, t_hi, tq_at_cmd=tq_at_cmd)
    if len(est_t) < 3:
        return None

    max_tq = max(tq_at_cmd) if tq_at_cmd else 0.0
    knee_hit = max_tq > 40.0
    results = {"peak_t": t_peak, "peak_v": v_peak, "window_s": window_s,
               "n_pos": len(pos), "sensor_dps_per_pct": SENSOR_DPS_PER_PCT,
               "max_tq": max_tq, "knee_hit": knee_hit,
               "est_naive": (est_t_naive, est_deg_naive)}
    for sig, pts in pos:
        seg = [p for p in pts if t_lo <= p[0] <= t_hi]
        if len(seg) < 3:
            continue
        ts_r, vs_r = zip(*seg)
        v0 = vs_r[0]
        vs_r0 = [v - v0 for v in vs_r]

        def _fit(est_t_, est_deg_):
            est_at_r = interp(est_t_, est_deg_, ts_r)
            num = sum(x * y for x, y in zip(est_at_r, vs_r0))
            den = sum(x * x for x in est_at_r)
            if den < 1e-12:
                return None
            k = num / den
            residuals = [y - k * x for x, y in zip(est_at_r, vs_r0)]
            rms = math.sqrt(sum(r * r for r in residuals) / len(residuals))
            pk = max((abs(v) for v in est_at_r), default=0.0)
            return {"k": k, "abs_k": abs(k), "rms_resid": rms,
                    "peak_est_deg": pk}

        icd   = _fit(est_t, est_deg)
        naive = _fit(est_t_naive, est_deg_naive)
        if icd is None or naive is None:
            continue
        results[sig.split(".")[0]] = {
            **icd, "n": len(seg),
            "k_naive": naive["k"], "abs_k_naive": naive["abs_k"],
            "rms_naive": naive["rms_resid"],
        }
    return results


def main():
    window_s = float(sys.argv[1]) if len(sys.argv) > 1 else 30.0
    print(f"Sensor deg/s per %cmd = {SENSOR_DPS_PER_PCT:.4f} "
          f"(motor {MAX_PINION_RPM} RPM / gear {GEAR_RATIO} / sensor-gear "
          f"{CAPSTAN_TO_SENSOR_RATIO})")
    print(f"Window: +/-{window_s:.0f} s around torque peak\n")

    rows = []
    for d in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, d)
        if not os.path.isdir(p):
            continue
        r = analyze_sortie(p, window_s)
        if r is None:
            continue
        rows.append((d, r))

    if not rows:
        print("No sorties have Elev_Trim_Tab_Capstan_Pos in hires JSON yet.")
        return

    print(f"{'Sortie':<18} {'Pk%':>5} {'MaxTq%':>7} {'Knee':>5}  "
          f"{'Lane':<6} {'|k|naive':>8} {'|k|ICD':>7}  "
          f"{'RMS_ICD':>8}  {'PkEst':>7}  N")
    print("-" * 105)
    all_ks_icd, all_ks_naive = [], []
    for name, r in rows:
        pk = r["peak_v"]
        maxtq = r.get("max_tq", 0.0)
        knee  = "YES" if r.get("knee_hit") else "no"
        first = True
        for lane in sorted(k for k in r if k.startswith("RDC")):
            d = r[lane]
            print(f"{name if first else '':<18} "
                  f"{(f'{pk:.1f}' if first else ''):>5} "
                  f"{(f'{maxtq:.1f}' if first else ''):>7} "
                  f"{(knee if first else ''):>5}  "
                  f"{lane:<6} {d['abs_k_naive']:>8.4f} {d['abs_k']:>7.4f}  "
                  f"{d['rms_resid']:>8.3f}  {d['peak_est_deg']:>7.2f}  {d['n']}")
            all_ks_icd.append(d["abs_k"])
            all_ks_naive.append(d["abs_k_naive"])
            first = False

    def _stats(vals):
        m = sum(vals) / len(vals)
        v = sum((x - m) ** 2 for x in vals) / len(vals)
        return m, math.sqrt(v), min(vals), max(vals)

    if all_ks_icd:
        print("-" * 105)
        m, s, lo, hi = _stats(all_ks_naive)
        print(f"|k| naive : mean {m:.4f}  std {s:.4f}  min {lo:.4f}  max {hi:.4f}  "
              f"(n={len(all_ks_naive)})")
        m, s, lo, hi = _stats(all_ks_icd)
        print(f"|k| ICD   : mean {m:.4f}  std {s:.4f}  min {lo:.4f}  max {hi:.4f}  "
              f"(n={len(all_ks_icd)})")


if __name__ == "__main__":
    main()
