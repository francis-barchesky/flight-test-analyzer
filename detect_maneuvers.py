#!/usr/bin/env python3
"""
detect_maneuvers.py — scan sortie directories for flight-dynamics maneuvers.

Reads the downsampled surface time series from the `flight_plots` field of
rs_analysis_*.json.gz files inside each sortie directory under data/.

Detects per control axis (pitch / roll / yaw):
  3211    — 3-2-1-1 frequency sweep: 4 alternating pulses at ~3:2:1:1 durations
  doublet — 2 alternating equal-duration pulses (~1:1)
  step    — single sustained deflection held >= 3 s

Usage:
    python detect_maneuvers.py
    python detect_maneuvers.py --sortie S143
    python detect_maneuvers.py --out maneuvers.json
    python detect_maneuvers.py --thr-pitch 2.0 --thr-roll 0.8 --thr-yaw 0.8

Output:
    maneuvers.json   — catalog of every detection across all sorties
"""

import argparse
import gzip
import json
import glob
import os
import re
import sys

import numpy as np

ROOT      = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(ROOT, "data")

DEFAULT_DECK_INDEX = os.path.join(ROOT, "deck_index.json")

# Surface signal keys in flight_plots
SURF_KEYS = {
    "pitch":  "Elevator_lvdt2deg",
    "roll_L": "Left_Aileron_lvdt2deg",
    "roll_R": "Right_Aileron_lvdt2deg",
    "yaw":    "Rudder_lvdt2deg",
}

# ── Detection thresholds (degrees) ────────────────────────────────────────────
DEFAULT_THR = {"pitch": 1.5, "roll": 0.8, "yaw": 0.8}

MIN_PULSE_S_ABS       = 1.0
MIN_SAMPLES_PER_PULSE = 3
RATIO_TOL_3211        = 0.35
RATIO_TOL_DOUBLET     = 0.50

DUR_WINDOW = {
    "3211":    (3.5,  30.0),
    "doublet": (1.5,  12.0),
    "singlet": (3.0,  90.0),
}


# ── File discovery ─────────────────────────────────────────────────────────────

def _find_rs_files(data_root, sortie_filter=None):
    pattern = os.path.join(data_root, "**", "rs_analysis_*.json.gz")
    files = sorted(glob.glob(pattern, recursive=True))
    if sortie_filter:
        files = [f for f in files if sortie_filter in f]
    return files


def _load_rs(path):
    """
    Load surface signals from rs_analysis_*.json.gz.
    Returns dict with label, path, duration_s, sample_hz, pitch/roll/yaw (t, y).
    Returns None if no surface signals are present.
    """
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return None

    fp = d.get("flight_plots", {})
    if not isinstance(fp, dict):
        return None

    def _load_key(key):
        pts = fp.get(key)
        if not pts or not isinstance(pts, list):
            return None, None
        arr = np.asarray(pts, dtype=float)
        if arr.ndim != 2 or arr.shape[1] < 2 or len(arr) < 4:
            return None, None
        t = arr[:, 0]
        y = arr[:, 1]
        order = np.argsort(t, kind="stable")
        t, y = t[order], y[order]
        mask = np.isfinite(t) & np.isfinite(y)
        return t[mask], y[mask]

    t_e,  y_e  = _load_key(SURF_KEYS["pitch"])
    t_aL, y_aL = _load_key(SURF_KEYS["roll_L"])
    t_aR, y_aR = _load_key(SURF_KEYS["roll_R"])
    t_r,  y_r  = _load_key(SURF_KEYS["yaw"])

    if t_e is None and t_aL is None and t_r is None:
        return None

    # Differential aileron
    if t_aL is not None and t_aR is not None:
        if not (len(t_aL) == len(t_aR) and np.allclose(t_aL, t_aR, atol=0.1)):
            from scipy.interpolate import interp1d
            f = interp1d(t_aR, y_aR, bounds_error=False, fill_value=0.0)
            y_aR_r = f(t_aL)
        else:
            y_aR_r = y_aR
        t_roll = t_aL
        y_roll = 0.5 * (y_aL - y_aR_r)
    elif t_aL is not None:
        t_roll, y_roll = t_aL, y_aL
    else:
        t_roll, y_roll = None, None

    dur = float(d.get("duration_s", 0))
    all_hz = []
    for t_sig in [t_e, t_aL, t_r]:
        if t_sig is not None and dur > 0:
            all_hz.append(len(t_sig) / dur)
    sample_hz = float(np.mean(all_hz)) if all_hz else 1.0

    # Label from filename: rs_analysis_S061_2.json.gz → S061_2
    label = os.path.splitext(os.path.splitext(os.path.basename(path))[0])[0]
    label = label.replace("rs_analysis_", "")

    return {
        "label":      label,
        "path":       path,
        "duration_s": dur,
        "sample_hz":  sample_hz,
        "pitch":      (t_e,    y_e)    if t_e    is not None else (None, None),
        "roll":       (t_roll, y_roll) if t_roll is not None else (None, None),
        "yaw":        (t_r,    y_r)    if t_r    is not None else (None, None),
    }


# ── Pulse extraction ───────────────────────────────────────────────────────────

def _extract_pulses(t, y, threshold, min_pulse_s):
    """
    Find contiguous segments where |y| > threshold.
    A sign flip inside an active region starts a new pulse immediately.
    Returns list of dicts: t_start, t_end, duration, sign, amplitude.
    """
    pulses    = []
    in_pulse  = False
    p_start_i = 0
    p_sign    = 0

    for i in range(len(t)):
        active   = abs(y[i]) > threshold
        cur_sign = int(np.sign(y[i]))

        if not in_pulse:
            if active:
                in_pulse  = True
                p_start_i = i
                p_sign    = cur_sign
        else:
            sign_flip = cur_sign != 0 and cur_sign != p_sign
            ending    = (not active) or sign_flip
            if ending:
                dur = float(t[i] - t[p_start_i])
                n   = i - p_start_i
                if dur >= min_pulse_s and n >= MIN_SAMPLES_PER_PULSE:
                    pulses.append({
                        "t_start":   float(t[p_start_i]),
                        "t_end":     float(t[i]),
                        "duration":  dur,
                        "sign":      p_sign,
                        "amplitude": float(np.max(np.abs(y[p_start_i:i]))),
                        "returned":  not active or sign_flip,
                    })
                if active:
                    p_start_i = i
                    p_sign    = cur_sign
                else:
                    in_pulse = False

    # Pulse still active at end of recording — did not return to neutral
    if in_pulse:
        i   = len(t) - 1
        dur = float(t[i] - t[p_start_i])
        n   = i - p_start_i
        if dur >= min_pulse_s and n >= MIN_SAMPLES_PER_PULSE:
            pulses.append({
                "t_start":   float(t[p_start_i]),
                "t_end":     float(t[i]),
                "duration":  dur,
                "sign":      p_sign,
                "amplitude": float(np.max(np.abs(y[p_start_i:]))),
                "returned":  False,
            })
    return pulses


def _alternating(signs):
    return all(signs[k] != signs[k + 1] for k in range(len(signs) - 1))


def _ratio_match(durs, ratios, tol):
    if len(durs) != len(ratios):
        return False
    unit = sum(durs) / sum(ratios)
    if unit <= 0:
        return False
    return all(abs(d - unit * r) / (unit * r) <= tol for d, r in zip(durs, ratios))


# ── Maneuver scanner ───────────────────────────────────────────────────────────

def _scan_axis(t, y, axis, threshold, min_pulse_s):
    pulses   = _extract_pulses(t, y, threshold, min_pulse_s)
    detected = []
    n = len(pulses)
    i = 0

    while i < n:
        # 3-2-1-1: 4 alternating pulses at ~3:2:1:1
        if i + 3 < n:
            grp   = pulses[i:i + 4]
            signs = [p["sign"] for p in grp]
            durs  = [p["duration"] for p in grp]
            total = sum(durs)
            span  = grp[-1]["t_end"] - grp[0]["t_start"]
            if (_alternating(signs)
                    and DUR_WINDOW["3211"][0] <= total <= DUR_WINDOW["3211"][1]
                    and _ratio_match(durs, [3.0, 2.0, 1.0, 1.0], RATIO_TOL_3211)
                    and span <= total * 1.4 + 2.0):
                unit = total / 7.0
                detected.append({
                    "type":       "3211",
                    "axis":       axis,
                    "t_start":    grp[0]["t_start"],
                    "t_end":      grp[-1]["t_end"],
                    "duration_s": round(total, 2),
                    "span_s":     round(span, 2),
                    "unit_s":     round(unit, 2),
                    "amplitudes": [round(p["amplitude"], 2) for p in grp],
                })
                i += 4
                continue

        # Doublet: 2 alternating pulses at ~1:1
        if i + 1 < n:
            grp   = pulses[i:i + 2]
            signs = [p["sign"] for p in grp]
            durs  = [p["duration"] for p in grp]
            total = sum(durs)
            span  = grp[-1]["t_end"] - grp[0]["t_start"]
            if (_alternating(signs)
                    and DUR_WINDOW["doublet"][0] <= total <= DUR_WINDOW["doublet"][1]
                    and _ratio_match(durs, [1.0, 1.0], RATIO_TOL_DOUBLET)
                    and span <= total * 1.4 + 2.0):
                detected.append({
                    "type":       "doublet",
                    "axis":       axis,
                    "t_start":    grp[0]["t_start"],
                    "t_end":      grp[-1]["t_end"],
                    "duration_s": round(total, 2),
                    "amplitudes": [round(p["amplitude"], 2) for p in grp],
                })
                i += 2
                continue

        # Singlet: single sustained deflection that returns to neutral
        p = pulses[i]
        if (p["returned"]
                and DUR_WINDOW["singlet"][0] <= p["duration"] <= DUR_WINDOW["singlet"][1]):
            detected.append({
                "type":       "singlet",
                "axis":       axis,
                "t_start":    p["t_start"],
                "t_end":      p["t_end"],
                "duration_s": round(p["duration"], 2),
                "amplitudes": [round(p["amplitude"], 2)],
            })

        i += 1

    return detected


def scan_file(rs_data, thresholds):
    hz          = rs_data["sample_hz"]
    min_pulse_s = max(MIN_PULSE_S_ABS, MIN_SAMPLES_PER_PULSE / max(hz, 0.1))

    all_detected = []
    for axis in ["pitch", "roll", "yaw"]:
        t, y = rs_data[axis]
        if t is None or len(t) < 6:
            continue
        found = _scan_axis(t, y, axis, thresholds[axis], min_pulse_s)
        all_detected.extend(found)

    return sorted(all_detected, key=lambda m: m["t_start"])


# ── Deck index lookup ─────────────────────────────────────────────────────────

def _load_deck_index(path):
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _deck_url(label, index):
    if not index:
        return None
    if label in index:
        return index[label]
    base = re.sub(r'_\d+$', '', label)
    return index.get(base)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--sortie",     help="Filter by sortie label substring (e.g. S143)")
    ap.add_argument("--out",        default="maneuvers.json")
    ap.add_argument("--deck-index", default=DEFAULT_DECK_INDEX, metavar="PATH",
                    help="JSON mapping sortie labels to flight deck URLs")
    ap.add_argument("--thr-pitch",  type=float, default=DEFAULT_THR["pitch"],  metavar="DEG")
    ap.add_argument("--thr-roll",   type=float, default=DEFAULT_THR["roll"],   metavar="DEG")
    ap.add_argument("--thr-yaw",    type=float, default=DEFAULT_THR["yaw"],    metavar="DEG")
    args = ap.parse_args()

    thresholds = {"pitch": args.thr_pitch, "roll": args.thr_roll, "yaw": args.thr_yaw}
    deck_index = _load_deck_index(args.deck_index)

    rs_files = _find_rs_files(DATA_ROOT, sortie_filter=args.sortie)
    if not rs_files:
        print(f"No rs_analysis_*.json.gz files found under {DATA_ROOT}")
        sys.exit(1)

    catalog          = {}
    type_total       = {}
    type_axis_total  = {}
    n_scanned        = 0
    n_skipped        = 0

    for path in rs_files:
        rs = _load_rs(path)
        if rs is None:
            n_skipped += 1
            continue

        n_scanned += 1
        label    = rs["label"]
        detected = scan_file(rs, thresholds)

        url = _deck_url(label, deck_index)
        if url and detected:
            for m in detected:
                m["deck_url"] = url

        if detected:
            if label not in catalog:
                catalog[label] = []
            catalog[label].extend(detected)

            by_axis = {"pitch": {}, "roll": {}, "yaw": {}}
            for m in detected:
                ax = m["axis"]
                t  = m["type"]
                by_axis[ax][t] = by_axis[ax].get(t, 0) + 1
                type_total[t]  = type_total.get(t, 0) + 1
                if t not in type_axis_total:
                    type_axis_total[t] = {}
                type_axis_total[t][ax] = type_axis_total[t].get(ax, 0) + 1

            axis_parts = []
            for ax in ["pitch", "roll", "yaw"]:
                if by_axis[ax]:
                    types_str = " ".join(f"{t}:{n}" for t, n in sorted(by_axis[ax].items()))
                    axis_parts.append(f"{ax}[{types_str}]")
            summary = "  ".join(axis_parts)
            hz_str  = f"{rs['sample_hz']:.1f}Hz"
            print(f"  [{label:20s}]  {len(detected):3d}  {summary}  {hz_str}")

    total = sum(type_total.values())
    print("\n" + "-"*65)
    print(f"  Scanned {n_scanned} sorties  ({n_skipped} skipped - no surface signals)")
    print(f"  Detected {total} maneuvers across {len(catalog)} sorties:")
    for t in ["3211", "doublet", "singlet"]:
        n = type_total.get(t, 0)
        if n:
            axis_counts = "  ".join(
                f"{ax}:{type_axis_total[t].get(ax, 0)}"
                for ax in ["pitch", "roll", "yaw"]
            )
            print(f"    {t:<10}  {n:4d}    ({axis_counts})")

    out_path = os.path.join(ROOT, args.out)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
