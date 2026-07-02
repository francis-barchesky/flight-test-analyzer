#!/usr/bin/env python3
"""Plot pitch-trim torque peak with command + position in a +/-30 s window.

For one sortie (default S118_2_N208B), finds the maximum |torque| across the
Pitch_Trim_Servo PTSRV TX_1/TX_2 Running_Avg signals, then renders stacked panels
around the peak:
  1) Pitch trim torque (both A429 lanes) with the peak marked
  2) Rate cmd + actual capstan position overlay (dual y-axis)
  3) Estimated capstan travel (integrating rate cmd through mechanical chain)
     vs actual capstan position change (both zeroed at window start)

Mechanical chain (constants):
  motor_pinion_RPM = (cmd / 100) * MAX_PINION_RPM       (MAX_PINION_RPM = 440)
  capstan_RPM      = motor_pinion_RPM / GEAR_RATIO      (GEAR_RATIO      = 8.995)
  cable_speed(in/s)= capstan_RPM / 60 * pi * CAPSTAN_DIA_IN   (CAPSTAN_DIA_IN = 2.25)
  cable_travel(in) = integral of cable_speed dt

Torque units: PTSRV Torque_Running_Avg is % of 25 in-lb at the pinion.
  pinion_inlb = (torque_pct / 100) * 25
  capstan_inlb = pinion_inlb * GEAR_RATIO   (ignoring gearbox loss)

Usage:
    py313 plot_pitch_trim_peak_window.py                # S118_2_N208B
    py313 plot_pitch_trim_peak_window.py S119_2_N208B   # different sortie
    py313 plot_pitch_trim_peak_window.py S118_2_N208B 45  # custom window (s)
"""
import json
import math
import os
import re
import sys

import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Pitch-trim servo mechanical constants ─────────────────────────────────────
MAX_PINION_RPM         = 440.0    # motor pinion RPM at 100% aptTrimRateServoCmd
GEAR_RATIO             = 8.995    # motor pinion rev / capstan rev
CAPSTAN_DIA_IN         = 2.25     # capstan diameter (inches)
CAPSTAN_TO_SENSOR_RATIO = 2.0645  # capstan rev / Elev_Trim_Tab_Capstan_Pos_sensor rev
                                  # (i.e. sensor sees 1/2.0645 of the capstan turn)

# Cable travel per 1% cmd per second
#   in/s per %cmd = (1/100) * MAX_PINION_RPM / GEAR_RATIO / 60 * pi * CAPSTAN_DIA_IN
CABLE_IPS_PER_PCT = (MAX_PINION_RPM / GEAR_RATIO / 60.0
                     * math.pi * CAPSTAN_DIA_IN / 100.0)

# Actual capstan angular rate (deg/s per %cmd) — before the sensor gear
CAPSTAN_DPS_PER_PCT = (MAX_PINION_RPM / GEAR_RATIO * 360.0 / 60.0 / 100.0)

# Rate seen by the Elev_Trim_Tab_Capstan_Pos sensor (deg/s per %cmd) —
# capstan angle divided by the additional 2.0645:1 gearing to the sensor
SENSOR_DPS_PER_PCT = CAPSTAN_DPS_PER_PCT / CAPSTAN_TO_SENSOR_RATIO

# ── Kaney ICD D-22999 Rev. B pg 41 — Reduced Requirement torque-speed curve ──
# Breakpoints picked from the nominal (black) curve of pinion speed [RPM]
# vs pinion torque [% of 25 in-lb]. Flat 440 RPM up to ~40% then droops to
# stall at 100%.  Motor rate is min(commanded, this-curve) at each instant.
ICD_TORQUE_SPEED = [
    (  0.0, 440.0),
    ( 40.0, 440.0),
    ( 50.0, 410.0),
    ( 60.0, 400.0),
    ( 70.0, 380.0),
    ( 80.0, 350.0),
    ( 85.0, 300.0),
    ( 90.0, 250.0),
    ( 95.0, 100.0),
    (100.0,   0.0),
    (200.0,   0.0),
]


def motor_max_rpm(torque_pct, curve=ICD_TORQUE_SPEED):
    """Nominal max pinion RPM available at |torque| (%) from ICD curve."""
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
CMD_RE = re.compile(r"aptTrimRateServoCmd$", re.I)
POS_RE = re.compile(r"Elev_Trim_Tab_Capstan_Pos\.?$", re.I)
CAS_RE = re.compile(r"casVoted$", re.I)

# ── CAS-scheduled trim-rate table from PDI_caravan.yaml ─────────────────────
#   pdiAptTrimRateCasBp:   [ 85.0, 100.0, 135.0 ]
#   pdiAptTrimRateTable:   [ 15.0, 13.0,   9.0 ]
# Below 85 KIAS the rate is held at 15%; above 135 KIAS held at 9%.
APT_TRIM_RATE_CAS_BP   = [85.0, 100.0, 135.0]
APT_TRIM_RATE_TABLE    = [15.0,  13.0,   9.0]


def scheduled_max_rate(cas):
    bp, tb = APT_TRIM_RATE_CAS_BP, APT_TRIM_RATE_TABLE
    if cas <= bp[0]:
        return tb[0]
    if cas >= bp[-1]:
        return tb[-1]
    for i in range(1, len(bp)):
        if cas <= bp[i]:
            frac = (cas - bp[i - 1]) / (bp[i] - bp[i - 1])
            return tb[i - 1] + frac * (tb[i] - tb[i - 1])
    return tb[-1]

DEFAULT_SORTIE = "S118_2_N208B"
DEFAULT_WINDOW_S = 30.0


def find_main_json(sortie_dir):
    for f in os.listdir(sortie_dir):
        if f.startswith("analysis") and f.endswith(".json") and "_hires" not in f:
            return os.path.join(sortie_dir, f)
    return None


def find_hires_json(sortie_dir):
    for f in os.listdir(sortie_dir):
        if f.endswith("_hires.json"):
            return os.path.join(sortie_dir, f)
    return None


def merged_plots(hires):
    """flight_plots + takeoff_plots, sorted by time per signal."""
    plots = {}
    for key in ("flight_plots", "takeoff_plots", "full_flight_torque"):
        for sig, pts in (hires.get(key) or {}).items():
            plots.setdefault(sig, []).extend(pts)
    for sig in plots:
        plots[sig].sort(key=lambda p: p[0])
    return plots


def extract(plots, pred):
    out = []
    for sig, pts in plots.items():
        if pred(sig) and pts:
            out.append((sig, pts))
    return out


def find_peak(ptrim_series):
    """Return (time, value, signal) of the largest |torque|."""
    best = (None, 0.0, None)
    for sig, pts in ptrim_series:
        for t, v in pts:
            if v is None:
                continue
            if abs(v) > abs(best[1]):
                best = (t, v, sig)
    return best


def slice_window(pts, t_lo, t_hi):
    return [(t, v) for (t, v) in pts if t_lo <= t <= t_hi]


def short_label(sig):
    m = re.search(r"PTSRV_A429_TX_(\d)", sig)
    if m:
        return f"PTSRV TX_{m.group(1)}"
    m = re.search(r"RDC(\d)", sig)
    if m:
        return f"RDC{m.group(1)} Capstan"
    return sig.split(".")[-1]


def main():
    sortie = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SORTIE
    window_s = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_WINDOW_S

    sortie_dir = os.path.join(ROOT, sortie)
    if not os.path.isdir(sortie_dir):
        print(f"ERROR: sortie dir not found: {sortie_dir}")
        sys.exit(1)

    main_json = find_main_json(sortie_dir)
    hires_json = find_hires_json(sortie_dir)
    if not main_json or not hires_json:
        print(f"ERROR: missing analysis or hires JSON in {sortie_dir}")
        sys.exit(1)

    with open(hires_json, encoding="utf-8") as f:
        hires = json.load(f)
    plots = merged_plots(hires)

    ptrim = extract(plots, lambda s: any(rx.search(s) for rx in PTRIM_PATTERNS))
    if not ptrim:
        print(f"ERROR: no pitch-trim torque series in {hires_json}")
        sys.exit(1)
    t_peak, v_peak, sig_peak = find_peak(ptrim)
    if t_peak is None:
        print("ERROR: could not locate torque peak")
        sys.exit(1)
    t_lo, t_hi = t_peak - window_s, t_peak + window_s

    cmd = extract(plots, lambda s: CMD_RE.search(s))
    pos = extract(plots, lambda s: POS_RE.search(s))

    print(f"Sortie     : {sortie}")
    print(f"Peak signal: {sig_peak}")
    print(f"Peak value : {v_peak:.3f}%  ({v_peak*0.25:.4f} in-lb pinion)")
    print(f"Peak time  : {t_peak:.3f} s")
    print(f"Window     : [{t_lo:.3f}, {t_hi:.3f}]  (+/-{window_s:.0f} s)")
    print(f"Cmd series : {len(cmd)}  Pos series: {len(pos)}")

    print(f"Cable travel   : {CABLE_IPS_PER_PCT:.6f} in/s per %cmd  "
          f"(= {CABLE_IPS_PER_PCT*100:.4f} in/s at 100%)")
    print(f"Capstan ang    : {CAPSTAN_DPS_PER_PCT:.4f} deg/s per %cmd  "
          f"(= {CAPSTAN_DPS_PER_PCT*100:.2f} deg/s at 100%)")
    print(f"Sensor  ang    : {SENSOR_DPS_PER_PCT:.4f} deg/s per %cmd  "
          f"(via extra {CAPSTAN_TO_SENSOR_RATIO}:1 gear to sensor)")

    n_panels = 2 + (3 if pos else 0)
    fig, axes = plt.subplots(n_panels, 1, figsize=(11, 2.6 * n_panels + 0.4),
                             sharex=True, squeeze=False)
    axes = axes.flat

    # ── Panel 1: Pitch trim torque ──────────────────────────────────────────
    ax = axes[0]
    torque_colors = ["#9A6BC4", "#5E3D8C", "#C798E0"]
    for i, (sig, pts) in enumerate(ptrim):
        seg = slice_window(pts, t_lo, t_hi)
        if not seg:
            continue
        ts, vs = zip(*seg)
        ax.plot(ts, vs, color=torque_colors[i % len(torque_colors)], lw=0.9,
                label=short_label(sig))
    # 25 in-lb = 100% at pinion; horizontal reference lines at ±100%
    ax.axhline(100, color="#E07070", lw=0.6, ls="--", alpha=0.6)
    ax.axhline(-100, color="#E07070", lw=0.6, ls="--", alpha=0.6)
    ax.plot([t_peak], [v_peak], "o", color="#E08040", ms=8, mec="white", mew=1.0,
            zorder=5, label=f"peak {v_peak:.2f}%")
    ax.annotate(f"{v_peak:.2f}%  ({v_peak*0.25:.3f} in-lb pinion)  @ {t_peak:.2f}s",
                xy=(t_peak, v_peak), xytext=(8, 6),
                textcoords="offset points", fontsize=9, color="#E08040",
                fontweight="bold")
    ax.axvline(t_peak, color="#E08040", lw=0.5, ls=":", alpha=0.6)
    ax.set_ylabel("Pinion torque (% of 25 in-lb)")
    ax.set_title(f"{sortie}  —  pitch-trim torque peak (+/-{window_s:.0f}s window)  "
                 f"[100% = 25 in-lb at pinion]",
                 fontsize=11)
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8, loc="upper right")

    # Helper: extract windowed cmd series (single first lane) as arrays
    cmd_ts, cmd_vs = [], []
    if cmd:
        seg = slice_window(cmd[0][1], t_lo, t_hi)
        if seg:
            cmd_ts, cmd_vs = zip(*seg)
            cmd_ts, cmd_vs = list(cmd_ts), list(cmd_vs)

    # ── Panel 2: Rate cmd + actual capstan position OVERLAY + CAS schedule ──
    ax = axes[1]
    if cmd_ts:
        ax.plot(cmd_ts, cmd_vs, color="#A8D8A8", lw=1.0,
                label="aptTrimRateServoCmd (%)")
        ax.axhline(0, color="#3a3a37", lw=0.6, ls=":", alpha=0.6)

    # CAS-schedule envelope: ±scheduled_max_rate(CAS(t))
    cas_series = [(s, pts) for s, pts in plots.items()
                  if CAS_RE.search(s) and pts]
    if cas_series:
        seg_cas = slice_window(cas_series[0][1], t_lo, t_hi)
        if seg_cas:
            ts_c = [p[0] for p in seg_cas]
            vs_c = [p[1] for p in seg_cas]
            sched = [scheduled_max_rate(c) for c in vs_c]
            ax.plot(ts_c, sched,         color="#B060B0", lw=0.9, ls=":",
                    alpha=0.85, label="+schedule (CAS)")
            ax.plot(ts_c, [-s for s in sched], color="#B060B0", lw=0.9, ls=":",
                    alpha=0.85)

    ax.set_ylabel("Rate cmd (%)", color="#5FA870")
    ax.tick_params(axis="y", colors="#5FA870")
    ax.axvline(t_peak, color="#E08040", lw=0.5, ls=":", alpha=0.6)
    ax.grid(True, alpha=0.25)

    ax_cas = None
    if cas_series:
        seg_cas = slice_window(cas_series[0][1], t_lo, t_hi)
        if seg_cas:
            ax_cas = ax.twinx()
            # Offset the CAS axis outward so it doesn't collide with capstan-pos axis
            ax_cas.spines["right"].set_position(("axes", 1.08))
            ts_c = [p[0] for p in seg_cas]
            vs_c = [p[1] for p in seg_cas]
            ax_cas.plot(ts_c, vs_c, color="#B0A040", lw=0.8, alpha=0.7,
                        label="CAS (kt)")
            ax_cas.set_ylabel("CAS (kt)", color="#B0A040")
            ax_cas.tick_params(axis="y", colors="#B0A040")
            for bp in APT_TRIM_RATE_CAS_BP:
                ax_cas.axhline(bp, color="#B0A040", lw=0.3, ls=":", alpha=0.4)

    if pos:
        ax_r = ax.twinx()
        pos_colors = ["#4CC9A0", "#2A8060"]
        for i, (sig, pts) in enumerate(pos):
            seg = slice_window(pts, t_lo, t_hi)
            if not seg:
                continue
            ts, vs = zip(*seg)
            ax_r.plot(ts, vs, color=pos_colors[i % len(pos_colors)], lw=1.0,
                      label=short_label(sig))
        ax_r.set_ylabel("Capstan Pos (raw)", color="#2A8060")
        ax_r.tick_params(axis="y", colors="#2A8060")
        handles = [ax, ax_r]
        if ax_cas is not None:
            handles.append(ax_cas)
        merged_h, merged_l = [], []
        for a in handles:
            h, l = a.get_legend_handles_labels()
            merged_h += h
            merged_l += l
        ax.legend(merged_h, merged_l, fontsize=8, loc="upper right")
    else:
        ax.legend(fontsize=8, loc="upper right")

    # ── Panel 3: Estimated vs actual capstan position CHANGE (deg, same axis) ─
    # Elev_Trim_Tab_Capstan_Pos is reported in DEGREES (capstan angle).
    # Integrate cmd (%) * CAPSTAN_DPS_PER_PCT to get expected capstan degrees.
    if pos:
        ax = axes[2]

        # Integrate cmd (%) through motor + capstan gear + sensor gear -> degrees
        # at the Elev_Trim_Tab_Capstan_Pos sensor (matches signal units).
        #
        # NEW: apply the Kaney ICD torque-speed curve.  At each step the motor's
        # available speed is limited by motor_max_rpm(|torque|); the commanded
        # rate saturates against that ceiling.  Below the knee (~40 %) this
        # equals the naive constant-scaling case.

        # Build a torque time-series interpolated at cmd timestamps: |max of the
        # two PTSRV lanes|.
        tq_at_cmd = [0.0] * len(cmd_ts) if cmd_ts else []
        for sig, pts in ptrim:
            seg = [p for p in pts if t_lo - 1 <= p[0] <= t_hi + 1]
            if not seg:
                continue
            ts_t = [p[0] for p in seg]
            vs_t = [p[1] for p in seg]
            j = 0
            for i, t in enumerate(cmd_ts):
                while j + 1 < len(ts_t) and ts_t[j + 1] < t:
                    j += 1
                if j + 1 >= len(ts_t) or ts_t[j] > t:
                    v = vs_t[j]
                elif ts_t[j + 1] == ts_t[j]:
                    v = vs_t[j]
                else:
                    frac = (t - ts_t[j]) / (ts_t[j + 1] - ts_t[j])
                    v = vs_t[j] + frac * (vs_t[j + 1] - vs_t[j])
                tq_at_cmd[i] = max(tq_at_cmd[i], abs(v))

        est_ts, est_deg = [], []
        est_ts_naive, est_deg_naive = [], []
        if cmd_ts:
            acc = 0.0
            acc_naive = 0.0
            est_ts.append(cmd_ts[0]);       est_deg.append(0.0)
            est_ts_naive.append(cmd_ts[0]); est_deg_naive.append(0.0)
            for i in range(1, len(cmd_ts)):
                dt = cmd_ts[i] - cmd_ts[i - 1]
                cmd_avg = 0.5 * (cmd_vs[i] + cmd_vs[i - 1])
                tq_avg  = 0.5 * (tq_at_cmd[i] + tq_at_cmd[i - 1])

                # ICD-corrected: cap motor RPM at what the curve allows at |tq|
                cmd_rpm    = cmd_avg / 100.0 * MAX_PINION_RPM
                cap_rpm    = motor_max_rpm(tq_avg)
                if abs(cmd_rpm) > cap_rpm:
                    actual_rpm = math.copysign(cap_rpm, cmd_rpm)
                else:
                    actual_rpm = cmd_rpm
                # Convert pinion RPM -> sensor deg/s
                sensor_dps = (actual_rpm * 360.0 / 60.0
                              / GEAR_RATIO / CAPSTAN_TO_SENSOR_RATIO)
                acc += sensor_dps * dt

                # Naive (no torque curve) for side-by-side comparison
                acc_naive += cmd_avg * SENSOR_DPS_PER_PCT * dt

                est_ts.append(cmd_ts[i]);       est_deg.append(acc)
                est_ts_naive.append(cmd_ts[i]); est_deg_naive.append(acc_naive)

        # Report min/max torque seen and whether the curve ever bit
        if tq_at_cmd:
            max_tq = max(tq_at_cmd)
            hit_knee = any(t > 40.0 for t in tq_at_cmd)
            print(f"  |torque| in window: max {max_tq:.2f}%  "
                  f"(curve knee at 40% — {'HIT' if hit_knee else 'not reached'})")

        # Zero each actual position series (deg) at window start
        actual_zeroed = []
        for sig, pts in pos:
            seg = slice_window(pts, t_lo, t_hi)
            if not seg:
                continue
            ts, vs = zip(*seg)
            v0 = vs[0]
            actual_zeroed.append((sig, list(ts), [v - v0 for v in vs]))

        # Best sign fit: check if actual moves opposite to est (positive cmd
        # driving position down is a common convention). If so, flip est sign
        # so both curves compare on the same axis.
        sign = 1.0
        k = None
        if est_ts and actual_zeroed:
            ts_r, vs_r0 = actual_zeroed[0][1], actual_zeroed[0][2]
            j = 0
            est_at_r = []
            for t in ts_r:
                while j + 1 < len(est_ts) and est_ts[j + 1] < t:
                    j += 1
                if j + 1 >= len(est_ts) or est_ts[j] > t:
                    est_at_r.append(est_deg[j])
                else:
                    t0e, t1e = est_ts[j], est_ts[j + 1]
                    v0e, v1e = est_deg[j], est_deg[j + 1]
                    frac = (t - t0e) / (t1e - t0e) if t1e > t0e else 0.0
                    est_at_r.append(v0e + frac * (v1e - v0e))
            num = sum(x * y for x, y in zip(est_at_r, vs_r0))
            den = sum(x * x for x in est_at_r)
            if den > 1e-12:
                k = num / den   # signed deg_actual / deg_est
                sign = -1.0 if k < 0 else 1.0
                print(f"Actual/Est ratio: {k:.3f}  (|k| < 1 => actual "
                      f"moves less than predicted; sign flip: {sign < 0})")
        k_mag = abs(k) if k is not None else 1.0

        pos_colors = ["#4CC9A0", "#2A8060"]
        for i, (sig, ts, vs) in enumerate(actual_zeroed):
            ax.plot(ts, vs, color=pos_colors[i % len(pos_colors)], lw=1.0,
                    label=f"{short_label(sig)} actual (deg)")
        if est_ts:
            est_plot = [sign * v for v in est_deg]
            est_scaled = [sign * k_mag * v for v in est_deg]
            est_naive_plot = ([sign * v for v in est_deg_naive]
                              if est_deg_naive else [])
            sign_tag = "-" if sign < 0 else "+"
            if est_naive_plot:
                ax.plot(est_ts_naive, est_naive_plot, color="#8890E0",
                        lw=1.0, ls="-.", alpha=0.75,
                        label=f"naive (no torque curve)")
            ax.plot(est_ts, est_plot, color="#E0A040", lw=1.4, ls="--",
                    label=f"ICD-corrected ({sign_tag}∫)")
            ax.plot(est_ts, est_scaled, color="#D06070", lw=1.2, ls=":",
                    label=f"ICD × {k_mag:.3f} (fit)")

        ax.set_ylabel("Capstan Δpos (deg)")
        ax.axvline(t_peak, color="#E08040", lw=0.5, ls=":", alpha=0.6)
        ax.axhline(0, color="#3a3a37", lw=0.6, ls=":", alpha=0.6)
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8, loc="upper right")
        ax.set_title(f"Estimated capstan travel (integrated cmd) vs actual "
                     f"position change  —  both in degrees",
                     fontsize=10)

        # ── Panel 4: Residual (actual − estimated) ──────────────────────────
        ax = axes[3]
        if est_ts and actual_zeroed:
            for i, (sig, ts, vs) in enumerate(actual_zeroed):
                # Sample estimated onto this series' timestamps
                j = 0
                resid_raw, resid_scaled, resid_t = [], [], []
                for t, v in zip(ts, vs):
                    while j + 1 < len(est_ts) and est_ts[j + 1] < t:
                        j += 1
                    if j + 1 >= len(est_ts) or est_ts[j] > t:
                        e = est_deg[j]
                    else:
                        t0e, t1e = est_ts[j], est_ts[j + 1]
                        v0e, v1e = est_deg[j], est_deg[j + 1]
                        frac = (t - t0e) / (t1e - t0e) if t1e > t0e else 0.0
                        e = v0e + frac * (v1e - v0e)
                    resid_t.append(t)
                    resid_raw.append(v - sign * e)
                    resid_scaled.append(v - sign * k_mag * e)
                ax.plot(resid_t, resid_raw, color=pos_colors[i % len(pos_colors)],
                        lw=1.0, label=f"{short_label(sig)}  actual − est")
                ax.plot(resid_t, resid_scaled, color=pos_colors[i % len(pos_colors)],
                        lw=1.0, ls="--", alpha=0.8,
                        label=f"{short_label(sig)}  actual − est×{k_mag:.3f}")
            ax.axhline(0, color="#3a3a37", lw=0.6, ls=":", alpha=0.6)
        ax.axvline(t_peak, color="#E08040", lw=0.5, ls=":", alpha=0.6)
        ax.set_ylabel("Residual (deg)")
        ax.grid(True, alpha=0.25)
        if ax.get_legend_handles_labels()[1]:
            ax.legend(fontsize=7, loc="upper right", ncol=2)
        ax.set_title("Position tracking error: actual − (sign · integrated cmd)  "
                     "[solid = raw, dashed = fit-scaled]",
                     fontsize=10)

        # ── Panel 5: Cmd expressed as physical rates ────────────────────────
        ax = axes[4]
        if cmd_ts:
            rate_ips = [v * CABLE_IPS_PER_PCT for v in cmd_vs]
            rate_dps = [v * CAPSTAN_DPS_PER_PCT for v in cmd_vs]
            ax.plot(cmd_ts, rate_ips, color="#E0A040", lw=1.0,
                    label="cable rate (in/s)")
            ax.axhline(0, color="#3a3a37", lw=0.6, ls=":", alpha=0.6)
            ax.set_ylabel("Cable rate (in/s)", color="#E0A040")
            ax.tick_params(axis="y", colors="#E0A040")

            ax_r = ax.twinx()
            ax_r.plot(cmd_ts, rate_dps, color="#8890E0", lw=0.8, alpha=0.7,
                      label="capstan rate (deg/s)")
            ax_r.set_ylabel("Capstan rate (deg/s)", color="#8890E0")
            ax_r.tick_params(axis="y", colors="#8890E0")

            h1, l1 = ax.get_legend_handles_labels()
            h2, l2 = ax_r.get_legend_handles_labels()
            ax.legend(h1 + h2, l1 + l2, fontsize=8, loc="upper right")
        ax.axvline(t_peak, color="#E08040", lw=0.5, ls=":", alpha=0.6)
        ax.grid(True, alpha=0.25)
        ax.set_title(f"Cmd converted through gear ratio {GEAR_RATIO} + "
                     f"capstan Ø{CAPSTAN_DIA_IN}\"  (motor max {MAX_PINION_RPM:.0f} RPM)",
                     fontsize=10)

    axes[-1].set_xlabel(f"time (s)  [peak at {t_peak:.2f}]")

    fig.tight_layout()
    out = os.path.join(ROOT, f"pitch_trim_peak_{sortie}.png")
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
