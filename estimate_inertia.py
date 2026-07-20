"""
estimate_inertia.py — two-stage hierarchical roll inertia estimator.

Roll equation:  Ixx_total(t) · p_dot = qSb · (Cl_da·da + Cl_p·pb/2V)
    with Cl_p < 0 (roll damping), Cl_da sign determined by data.

Define:
    |Cl_p|  = -Cl_p  (positive)
    Psi_i   = Ixx_total_i / |Cl_p|   (per-sortie)
    eta     = Cl_da / |Cl_p|          (global, sign from data)

Roll equation becomes:
    Psi_i · p_dot = qSb · (eta·da - pb/2V)

Rearranged as linear system (Stage 1):
    y = -qSb·pb/2V
    [ṗ_sortiei | -qSb·da] · [Psi_i | eta] = y

The per-sortie ṗ column is zero for all other sorties, so the normal equations
for Psi_i and eta are separable: steady-roll samples constrain eta (when p_dot ≈ 0),
maneuver samples constrain Psi_i (when p_dot is large).

Stage 2:
    Psi_i = Ixx_ZFW/|Cl_p| + (sigma/|Cl_p|) · m_fuel_i

    slope    = sigma/|Cl_p|   =>  |Cl_p|   = sigma / slope
    intercept = Ixx_ZFW/|Cl_p|  =>  Ixx_ZFW = intercept * |Cl_p|
    Cl_da   = eta * |Cl_p|

The known tank-arm geometry (sigma = y_tank^2) calibrates absolute |Cl_p|
without any aerodynamic prior.

Usage:
    python estimate_inertia.py
    python estimate_inertia.py --sigma 49.0      # fuel-arm^2 override (ft^2)
    python estimate_inertia.py --sortie S143_1   # single-sortie debug
    python estimate_inertia.py --out results.json
"""
import argparse
import glob
import json
import math
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import numpy as np
from scipy.interpolate import interp1d
from scipy.optimize import curve_fit
from scipy.signal import savgol_filter

# ── Aircraft geometry ──────────────────────────────────────────────────────────
S_FT2         = 279.0
B_FT          = 52.1
C_BAR_FT      = S_FT2 / B_FT          # 5.355 ft — mean aerodynamic chord
G_FPS2        = 32.174
KTS2FPS       = 1.68781
DEG2RAD       = math.pi / 180
RHO_SL        = 0.002377

Y_TANK_DEFAULT = 7.0
SIGMA_DEFAULT  = Y_TANK_DEFAULT ** 2   # 49 ft^2


def sigma_from_tank_bounds(y1_ft, y2_ft):
    """
    Effective y² for fuel mass uniformly distributed from y1 to y2 (ft from CL).
    Derived from ∫_{y1}^{y2} y² dy / (y2 - y1) = (y1² + y1·y2 + y2²) / 3.
    Both sides (left + right) contribute identically by symmetry.
    """
    return (y1_ft**2 + y1_ft * y2_ft + y2_ft**2) / 3.0

# ── Signal map ─────────────────────────────────────────────────────────────────
# All signals come from .afcs_signals.npz (built by extract_afcs.py v2).
# Logical name → cache key (same key as in AFCS_SIGNAL_MAP in extract_afcs.py).
AFCS_CACHE_FILE = ".afcs_signals.npz"

# Required signals — sortie rejected if any are absent.
REQUIRED_SIGS = {"p", "daL", "daR", "tas"}

ROOT = os.path.dirname(os.path.abspath(__file__))


# ── Loading ────────────────────────────────────────────────────────────────────

def _load_afcs(sortie_dir):
    """Load all signals from the AFCS cache. Returns {key: (t, y)} or None."""
    path = os.path.join(sortie_dir, AFCS_CACHE_FILE)
    if not os.path.exists(path):
        return None
    try:
        cache = np.load(path, allow_pickle=False)
    except Exception:
        return None
    # The cache stores logical keys directly (same as AFCS_SIGNAL_MAP keys)
    try:
        names_str = str(cache["_names"])
    except Exception:
        names_str = ""
    names = [n for n in names_str.split("|") if n]
    result = {}
    for key in names:
        tk, yk = f"{key}__t", f"{key}__y"
        if tk in cache and yk in cache:
            t = np.asarray(cache[tk], dtype=float)
            y = np.asarray(cache[yk], dtype=float)
            if t.ndim == 1 and len(t) > 2:
                result[key] = (t, y)
    return result if result else None


def _to_grid(t_grid, t_sig, y_sig):
    f = interp1d(t_sig, y_sig, bounds_error=False, fill_value=np.nan)
    return f(t_grid)


def _smooth_deriv(t, y, window_s=0.3, polyorder=3):
    dt  = t[1] - t[0]
    win = max(polyorder + 2, int(round(window_s / dt)) | 1)
    return savgol_filter(y, win, polyorder, deriv=1, delta=dt)


# ── Per-sortie regressor assembly ──────────────────────────────────────────────

def build_sortie_arrays(data, sg_window_s=0.3):
    """
    Build per-sortie arrays used in Stage 1.
    Returns dict with keys: p_dot, qSb, da_rad, pb_2V, rb_2V, beta_rad,
    dr_rad, m_fuel_slug, wt_lb, tas_fps, mask, pdot_rms_dps.
    """
    t = data["t"]
    g = data["grid"]

    p_rad   = g["p"] * DEG2RAD
    da_rad  = 0.5 * (g["daL"] - g["daR"]) * DEG2RAD
    dr_rad  = g.get("dr",  np.zeros_like(p_rad)) * DEG2RAD
    r_rad   = g.get("r",   np.zeros_like(p_rad)) * DEG2RAD

    tas     = g["tas"]          # ft/s
    h_ft    = g.get("press_alt", np.zeros_like(tas))
    T_R     = np.clip(518.67 - 3.5662e-3 * h_ft, 389.97, 518.67)
    rho     = RHO_SL * (T_R / 518.67) ** 4.2561
    q_bar   = 0.5 * rho * tas ** 2
    qSb     = q_bar * S_FT2 * B_FT

    with np.errstate(divide="ignore", invalid="ignore"):
        pb_2V = np.where(tas > 1.0, p_rad * B_FT / (2.0 * tas), np.nan)
        rb_2V = np.where(tas > 1.0, r_rad * B_FT / (2.0 * tas), np.nan)

    ay_arr   = g.get("ay", np.zeros_like(p_rad))
    beta_rad = -ay_arr / G_FPS2   # approx from lateral accel (ft/s^2)

    p_dot = _smooth_deriv(t, p_rad, window_s=sg_window_s)

    m_fuel  = (g["mL"] + g["mR"]) / G_FPS2   # slug (both tanks)
    wt      = g["wt"]
    cg      = g.get("cg", np.full(len(t), np.nan))

    mask = (
        np.isfinite(p_dot)   & np.isfinite(da_rad) & np.isfinite(pb_2V) &
        np.isfinite(rb_2V)   & np.isfinite(dr_rad) &
        np.isfinite(qSb)     & np.isfinite(beta_rad) &
        np.isfinite(m_fuel)  & np.isfinite(wt) &
        (wt   > 5000.0) &
        (cg   > 180.0)  & (cg < 215.0) &
        (tas  > 50 * KTS2FPS) &
        (np.abs(beta_rad) < np.deg2rad(10))
    )

    # Per-sortie roll excitation metric: RMS of p_dot (deg/s²) during airborne
    air_ok = mask & np.isfinite(p_dot)
    pdot_rms_dps = float(np.sqrt(np.mean(np.degrees(p_dot[air_ok]) ** 2))) if air_ok.sum() > 0 else 0.0

    return dict(
        p_rad        = p_rad,
        p_dot        = p_dot,
        qSb          = qSb,
        da_rad       = da_rad,
        pb_2V        = pb_2V,
        rb_2V        = rb_2V,
        beta_rad     = beta_rad,
        dr_rad       = dr_rad,
        m_fuel_slug  = m_fuel,
        wt_lb        = wt,
        tas_fps      = tas,
        mask         = mask,
        pdot_rms_dps = pdot_rms_dps,
    )


# ── Pitch-axis array builder ───────────────────────────────────────────────────

REQUIRED_SIGS_PITCH = {"q", "de", "tas"}


def build_pitch_arrays(data, sg_window_s=0.3):
    """
    Build per-sortie arrays for the PITCH equation Stage 1.

    Pitch equation:
        Iyy * q_dot = qSc [Cm_q * qc/(2V) + Cm_de * de + Cm_alpha * alpha + ...]
    Divide by |Cm_q|:
        Psi_p * q_dot = qSc * [qc/(2V) + xi * de]    (alpha treated as noise)

    Returns dict with keys: q_dot, qSc, de_rad, qc_2V, m_fuel_slug, wt_lb, mask.
    """
    t = data["t"]
    g = data["grid"]

    q_rad   = g["q"] * DEG2RAD                      # pitch rate, rad/s
    de_raw  = g["de"] * DEG2RAD                     # absolute elevator, rad
    # Incremental elevator: remove per-sortie trim to avoid trim-bias in regressor
    tas_mask = g["tas"] > 50 * KTS2FPS
    de_trim = float(np.nanmean(de_raw[tas_mask])) if tas_mask.sum() > 0 else 0.0
    de_rad  = de_raw - de_trim                      # incremental elevator

    tas     = g["tas"]                    # ft/s
    h_ft    = g.get("press_alt", np.zeros_like(tas))
    T_R     = np.clip(518.67 - 3.5662e-3 * h_ft, 389.97, 518.67)
    rho     = RHO_SL * (T_R / 518.67) ** 4.2561
    q_bar   = 0.5 * rho * tas ** 2       # dynamic pressure, psf
    qSc     = q_bar * S_FT2 * C_BAR_FT  # pitch moment arm, lb·ft

    with np.errstate(divide="ignore", invalid="ignore"):
        qc_2V = np.where(tas > 1.0, q_rad * C_BAR_FT / (2.0 * tas), np.nan)

    q_dot = _smooth_deriv(t, q_rad, window_s=sg_window_s)

    m_fuel = (g["mL"] + g["mR"]) / G_FPS2   # slug
    wt     = g["wt"]
    cg     = g.get("cg", np.full(len(t), np.nan))

    mask = (
        np.isfinite(q_dot)  & np.isfinite(de_rad) & np.isfinite(qc_2V) &
        np.isfinite(qSc)    & np.isfinite(m_fuel)  & np.isfinite(wt) &
        (wt  > 5000.0) &
        (cg  > 180.0)  & (cg < 215.0) &
        (tas > 50 * KTS2FPS)
    )

    return dict(
        q_dot      = q_dot,
        qSc        = qSc,
        de_rad     = de_rad,
        qc_2V      = qc_2V,
        m_fuel_slug= m_fuel,
        wt_lb      = wt,
        mask       = mask,
    )


def stage1_pitch(sorties_data, verbose=False):
    """
    Hierarchical Stage 1 for pitch axis.  One global parameter xi = Cm_de/|Cm_q|.

    y_j = -qSc_j * qc_2V_j
    Psi_p_i * q_dot_j + xi * (-qSc_j * de_j) = y_j

    Returns: theta, theta_se, resid_std, n_per_sortie
    """
    N_P = 1          # one global: xi
    N   = len(sorties_data)
    dim = N + N_P
    XtX = np.zeros((dim, dim))
    Xty = np.zeros(dim)

    n_per = []
    for i, sd in enumerate(sorties_data):
        arr  = sd["arrays_pitch"]
        mask = arr["mask"]
        if mask.sum() < 50:
            n_per.append(0); continue

        q_dot_m = arr["q_dot"][mask]
        qSc_m   = arr["qSc"][mask]
        de_m    = arr["de_rad"][mask]
        qc2V_m  = arr["qc_2V"][mask]

        ok = np.isfinite(q_dot_m) & np.isfinite(qSc_m) & np.isfinite(de_m) & np.isfinite(qc2V_m)
        n_ok = int(ok.sum())
        n_per.append(n_ok)
        if n_ok < 50:
            continue

        q_dot_m = q_dot_m[ok]; qSc_m = qSc_m[ok]; de_m = de_m[ok]; qc2V_m = qc2V_m[ok]
        y_m   = -qSc_m * qc2V_m
        G_m   = (-qSc_m * de_m).reshape(-1, 1)

        XtX[i, i]         += float(np.dot(q_dot_m, q_dot_m))
        cross              = float(q_dot_m @ G_m[:, 0])
        XtX[i, N]         += cross
        XtX[N, i]         += cross
        XtX[N, N]         += float(np.dot(G_m[:, 0], G_m[:, 0]))
        Xty[i]            += float(np.dot(q_dot_m, y_m))
        Xty[N]            += float(np.dot(G_m[:, 0], y_m))

    total_n = sum(n_per)
    if total_n < dim:
        return None, None, None, n_per

    try:
        theta = np.linalg.solve(XtX, Xty)
    except np.linalg.LinAlgError:
        theta = np.linalg.lstsq(XtX, Xty, rcond=None)[0]

    ss_res = 0.0; n_total = 0
    for i, sd in enumerate(sorties_data):
        arr  = sd["arrays_pitch"]
        mask = arr["mask"]
        if mask.sum() < 50: continue
        q_dot_m = arr["q_dot"][mask]; qSc_m = arr["qSc"][mask]
        de_m    = arr["de_rad"][mask]; qc2V_m = arr["qc_2V"][mask]
        ok = np.isfinite(q_dot_m) & np.isfinite(qSc_m) & np.isfinite(de_m) & np.isfinite(qc2V_m)
        if ok.sum() < 50: continue
        q_dot_m = q_dot_m[ok]; qSc_m = qSc_m[ok]; de_m = de_m[ok]; qc2V_m = qc2V_m[ok]
        y_m  = -qSc_m * qc2V_m
        G_m  = (-qSc_m * de_m).reshape(-1, 1)
        pred = theta[i] * q_dot_m + G_m[:, 0] * theta[N]
        res  = y_m - pred
        ss_res  += float(np.dot(res, res))
        n_total += int(ok.sum())

    dof    = max(1, n_total - dim)
    sigma2 = ss_res / dof
    try:
        cov      = sigma2 * np.linalg.pinv(XtX)
        theta_se = np.sqrt(np.abs(np.diag(cov)))
    except Exception:
        theta_se = np.full(dim, np.nan)

    resid_std = math.sqrt(sigma2) if n_total > dim else float("nan")
    return theta, theta_se, resid_std, n_per


# ── Global single-stage regression (time-varying fuel) ────────────────────────

def stage1_global(sorties_data, sigma_ft2, verbose=False):
    """
    Single-stage global regression.  Uses the time-varying m_fuel(t) signal
    directly — both within and between sorties — instead of per-sortie means.

    Define Psi(t) = Psi_ZFW + slope * m_fuel(t), where slope = sigma / |Cl_p|.
    The roll equation (divided by |Cl_p|, same as Stage 1) becomes:

        Psi_ZFW * p_dot + slope * m_fuel(t) * p_dot
            + eta * (-qSb*da) + mu_r * (-qSb*rb/2V)
            + mu_β * (-qSb*β) + mu_dr * (-qSb*dr)
            = -qSb * pb/2V

    Parameters (all global, no per-sortie unknowns):
        θ = [Psi_ZFW, slope, eta, mu_r, mu_β, mu_dr]

    Recovers:
        |Cl_p|   = sigma / slope
        Ixx_ZFW  = Psi_ZFW * |Cl_p|
        Cl_da    = eta * |Cl_p|

    Returns: theta, theta_se, resid_std, n_total
    """
    N_PAR = 6
    XtX   = np.zeros((N_PAR, N_PAR))
    Xty   = np.zeros(N_PAR)
    n_total = 0

    def _build(arr, mask):
        p_dot_m  = arr["p_dot"][mask]
        qSb_m    = arr["qSb"][mask]
        da_m     = arr["da_rad"][mask]
        pb2V_m   = arr["pb_2V"][mask]
        rb2V_m   = arr["rb_2V"][mask]
        beta_m   = arr["beta_rad"][mask]
        dr_m     = arr["dr_rad"][mask]
        m_fuel_m = arr["m_fuel_slug"][mask]
        ok = (np.isfinite(p_dot_m) & np.isfinite(qSb_m) & np.isfinite(da_m) &
              np.isfinite(pb2V_m) & np.isfinite(rb2V_m) & np.isfinite(beta_m) &
              np.isfinite(dr_m) & np.isfinite(m_fuel_m))
        if ok.sum() < 10:
            return None, None
        p_dot_m  = p_dot_m[ok];  qSb_m   = qSb_m[ok]
        da_m     = da_m[ok];     pb2V_m  = pb2V_m[ok]
        rb2V_m   = rb2V_m[ok];   beta_m  = beta_m[ok]
        dr_m     = dr_m[ok];     m_fuel_m = m_fuel_m[ok]
        X_m = np.column_stack([
            p_dot_m,               # Psi_ZFW
            m_fuel_m * p_dot_m,    # slope = sigma/|Cl_p|  (KEY: time-varying fuel)
            -qSb_m * da_m,         # eta   = Cl_da/|Cl_p|
            -qSb_m * rb2V_m,       # mu_r  = Cl_r/|Cl_p|
            -qSb_m * beta_m,       # mu_β  = Cl_β/|Cl_p|
            -qSb_m * dr_m,         # mu_dr = Cl_dr/|Cl_p|
        ])
        y_m = -qSb_m * pb2V_m      # same response as Stage 1
        return X_m, y_m

    for sd in sorties_data:
        arr  = sd["arrays"]
        mask = arr["mask"]
        if mask.sum() < 50:
            continue
        X_m, y_m = _build(arr, mask)
        if X_m is None:
            continue
        XtX += X_m.T @ X_m
        Xty += X_m.T @ y_m
        n_total += len(y_m)

    if n_total < N_PAR:
        return None, None, None, 0

    try:
        theta = np.linalg.solve(XtX, Xty)
    except np.linalg.LinAlgError:
        theta = np.linalg.lstsq(XtX, Xty, rcond=None)[0]

    ss_res = 0.0
    for sd in sorties_data:
        arr  = sd["arrays"]
        mask = arr["mask"]
        if mask.sum() < 50:
            continue
        X_m, y_m = _build(arr, mask)
        if X_m is None:
            continue
        res = y_m - X_m @ theta
        ss_res += float(np.dot(res, res))

    dof    = max(1, n_total - N_PAR)
    sigma2 = ss_res / dof
    try:
        cov      = sigma2 * np.linalg.pinv(XtX)
        theta_se = np.sqrt(np.abs(np.diag(cov)))
    except Exception:
        theta_se = np.full(N_PAR, np.nan)

    return theta, theta_se, math.sqrt(sigma2), n_total


# ── Output Error Method (JIO) ─────────────────────────────────────────────────

N_OEM    = 6  # [Ixx_ZFW, Cl_p, Cl_da, Cl_r, Cl_beta, Cl_dr]
N_OEM_JS = 7  # joint-sigma variant adds sigma as theta[6]


def _find_contiguous(mask, min_len=100):
    """Return [(i0, i1), ...] for contiguous True runs of length >= min_len."""
    segs = []
    n = len(mask)
    i = 0
    while i < n:
        if mask[i]:
            j = i + 1
            while j < n and mask[j]:
                j += 1
            if j - i >= min_len:
                segs.append((i, j))
            i = j
        else:
            i += 1
    return segs


def _oem_forward(sorties_data, theta, sigma_ft2, window_n, dt=0.01):
    """
    One OEM forward pass. For every window of window_n contiguous valid samples,
    reset p_pred to p_measured at the window start, integrate forward using the
    exact linear step, and accumulate Gauss-Newton normal equations.

    State vector tracked per window: [p, s0..s5] where s_k = dp_pred/dθ_k.
    Sensitivity ODEs are integrated simultaneously with p using exact linear steps.

    theta = [Ixx_ZFW (slug·ft²), Cl_p (<0, /rad), Cl_da, Cl_r, Cl_beta, Cl_dr]
      OR 7-element joint-sigma: theta[6] = sigma_ft2 (pass sigma_ft2=None to activate).
    Returns: JtJ, Jtr, cost (scalar), n_total (int)
    """
    joint = sigma_ft2 is None
    N_PAR = N_OEM_JS if joint else N_OEM
    JtJ = np.zeros((N_PAR, N_PAR))
    Jtr = np.zeros(N_PAR)
    cost = 0.0
    n_total = 0

    Ixx_ZFW, Cl_p, Cl_da, Cl_r, Cl_b, Cl_dr = theta[:6]
    if joint:
        sigma_ft2 = float(theta[6])

    for sd in sorties_data:
        arr  = sd["arrays"]
        mask = arr["mask"]

        p_meas = arr["p_rad"]
        qSb    = arr["qSb"]
        da     = arr["da_rad"]
        rb2V   = arr["rb_2V"]
        beta   = arr["beta_rad"]
        dr     = arr["dr_rad"]
        m_fuel = arr["m_fuel_slug"]
        tas    = arr["tas_fps"]

        # Collect start indices of all complete windows across contiguous segments
        win_starts = []
        for i0, i1 in _find_contiguous(mask, min_len=window_n):
            for w0 in range(i0, i1 - window_n + 1, window_n):
                win_starts.append(w0)
        if not win_starts:
            continue

        M = len(win_starts)
        W = window_n

        # Advanced indexing: gather (M, W) views of each signal
        idx   = np.array(win_starts)[:, np.newaxis] + np.arange(W)[np.newaxis, :]
        p_w   = p_meas[idx]
        qSb_w = qSb[idx]
        da_w  = da[idx]
        rb_w  = rb2V[idx]
        bet_w = beta[idx]
        dr_w  = dr[idx]
        mf_w  = m_fuel[idx]
        tas_w = np.maximum(tas[idx], 1.0)

        # Time-varying quantities, shape (M, W)
        Ixx_w  = Ixx_ZFW + sigma_ft2 * mf_w
        b2V_w  = B_FT / (2.0 * tas_w)
        A_w    = (qSb_w / Ixx_w) * Cl_p * b2V_w       # roll damping eigenvalue
        ea_w   = np.exp(np.clip(A_w * dt, -10.0, 1.0)) # exact step factor
        nz     = np.abs(A_w) > 1e-10
        inv_A  = np.where(nz, (ea_w - 1.0) / A_w, dt)  # (ea-1)/A or dt when A≈0
        aero_w = (qSb_w / Ixx_w) * (
            Cl_da * da_w + Cl_r * rb_w + Cl_b * bet_w + Cl_dr * dr_w
        )

        # Initial conditions: reset to measured p; sensitivities = 0
        p_curr = p_w[:, 0].copy()        # (M,)
        s      = np.zeros((N_PAR, M))    # (6, M)

        # Pre-allocate forcing buffer (avoids per-iter allocation)
        c = np.empty((N_PAR, M))

        for k in range(W - 1):
            A_k    = A_w[:, k]
            aero_k = aero_w[:, k]
            ea_k   = ea_w[:, k]
            iA_k   = inv_A[:, k]
            Ixx_k  = Ixx_w[:, k]
            qSb_k  = qSb_w[:, k]
            b2V_k  = b2V_w[:, k]

            pdot_k = A_k * p_curr + aero_k          # model p_dot (rad/s²)
            p_next = ea_k * p_curr + aero_k * iA_k  # exact linear step

            qSbI_k = qSb_k / Ixx_k
            c[0]   = -pdot_k / Ixx_k                # ∂/∂Ixx_ZFW
            c[1]   = qSbI_k * b2V_k * p_curr        # ∂/∂Cl_p
            c[2]   = qSbI_k * da_w[:, k]            # ∂/∂Cl_da
            c[3]   = qSbI_k * rb_w[:, k]            # ∂/∂Cl_r
            c[4]   = qSbI_k * bet_w[:, k]           # ∂/∂Cl_beta
            c[5]   = qSbI_k * dr_w[:, k]            # ∂/∂Cl_dr
            if joint:
                c[6] = c[0] * mf_w[:, k]            # ∂/∂sigma = ∂/∂Ixx_ZFW * m_fuel

            # Exact sensitivity step: s_next = ea*s + c*(ea-1)/A
            s_next = ea_k[np.newaxis, :] * s + c * iA_k[np.newaxis, :]

            # Residuals and normal-equation accumulation
            r_k = p_w[:, k + 1] - p_next            # (M,)
            J   = s_next.T                           # (M, 6)
            JtJ += J.T @ J
            Jtr += J.T @ r_k
            cost    += float(np.dot(r_k, r_k))
            n_total += M

            p_curr = p_next
            s      = s_next

    return JtJ, Jtr, cost, n_total


def stage_oem(sorties_data, sigma_ft2, theta0=None, max_iter=30,
              window_s=5.0, verbose=False):
    """
    Output Error Method (JIO): minimise ||p_measured - p_predicted||²
    over all sorties jointly via L-BFGS-B (scipy) with physical bounds.

    Seeds from stage1_global if theta0 is not provided.
    theta = [Ixx_ZFW (slug·ft²), Cl_p (/rad <0), Cl_da, Cl_r, Cl_beta, Cl_dr]

    Returns: theta, theta_se, resid_std_rads, n_total
    """
    from scipy.optimize import minimize as sp_minimize

    # ── seed from equation-error global regression ─────────────────────────────
    if theta0 is None:
        theta_g, _, _, _ = stage1_global(sorties_data, sigma_ft2)
        if theta_g is None:
            return None, None, None, 0
        slope_g = float(theta_g[1])
        if slope_g <= 0:
            return None, None, None, 0
        Cl_p_abs  = sigma_ft2 / slope_g
        Ixx_ZFW0  = float(theta_g[0]) * Cl_p_abs
        theta0 = np.array([
            Ixx_ZFW0,
            -Cl_p_abs,
            float(theta_g[2]) * Cl_p_abs,   # Cl_da
            float(theta_g[3]) * Cl_p_abs,   # Cl_r
            float(theta_g[4]) * Cl_p_abs,   # Cl_beta
            float(theta_g[5]) * Cl_p_abs,   # Cl_dr
        ])
    else:
        theta0 = np.asarray(theta0, dtype=float)

    dt       = float(sorties_data[0].get("dt", 0.01))
    window_n = max(10, int(round(window_s / dt)))

    if verbose:
        names = ["Ixx_ZFW", "Cl_p", "Cl_da", "Cl_r", "Cl_beta", "Cl_dr"]
        print(f"\n  OEM seed (from equation-error):")
        for nm, v in zip(names, theta0):
            print(f"    {nm:10s} = {v:+.4f}")
        print(f"  window = {window_n} samples ({window_s:.1f} s)")

    # ── physical bounds (keep Cl_p negative so roll is stable) ─────────────────
    bounds = [
        (100.0,  60000.0),   # Ixx_ZFW slug·ft²
        (-20.0,  -0.05),     # Cl_p  < 0
        (-2.0,    2.0),      # Cl_da
        (-2.0,    2.0),      # Cl_r
        (-2.0,    2.0),      # Cl_beta
        (-2.0,    2.0),      # Cl_dr
    ]

    # ── parameter scaling so L-BFGS-B sees O(1) parameter magnitudes ───────────
    # Scale = initial |θ_k| or 1.0 (avoids zero-scale)
    scale = np.maximum(np.abs(theta0), 0.01)

    call_count = [0]

    def _cost_grad(theta_s):
        """Cost and gradient in scaled parameter space."""
        theta = theta_s * scale
        call_count[0] += 1
        JtJ, Jtr, cost, n = _oem_forward(
            sorties_data, theta, sigma_ft2, window_n, dt=dt
        )
        if n == 0:
            return 1e30, np.zeros(N_OEM)
        # gradient of cost w.r.t. θ_s: chain rule via scale
        grad_theta  = -2.0 * Jtr           # ∂J/∂θ = -2 J^T r
        grad_scaled = grad_theta * scale    # ∂J/∂θ_s = ∂J/∂θ · ∂θ/∂θ_s = grad * scale
        rms = math.sqrt(cost / n)
        if verbose:
            print(f"  call {call_count[0]:3d}  "
                  f"rms={np.degrees(rms)*1000:.3f} m°/s  "
                  f"Ixx_ZFW={theta[0]:.0f}  Cl_p={theta[1]:.4f}")
        return float(cost), grad_scaled

    # Scaled bounds
    bounds_s = [(lo / sc, hi / sc) for (lo, hi), sc in zip(bounds, scale)]
    theta0_s = theta0 / scale

    result = sp_minimize(
        _cost_grad, theta0_s,
        method="L-BFGS-B", jac=True,
        bounds=bounds_s,
        options={"maxiter": max_iter * 10, "ftol": 1e-15, "gtol": 1e-8,
                 "maxfun": max_iter * 50},
    )

    theta = result.x * scale

    if verbose:
        print(f"\n  L-BFGS-B: {result.message}")

    # ── final pass for covariance ───────────────────────────────────────────────
    JtJ_f, _, cost_f, n_f = _oem_forward(
        sorties_data, theta, sigma_ft2, window_n, dt=dt
    )
    dof    = max(1, n_f - N_OEM)
    sigma2 = cost_f / dof
    try:
        cov      = sigma2 * np.linalg.pinv(JtJ_f)
        theta_se = np.sqrt(np.abs(np.diag(cov)))
    except Exception:
        theta_se = np.full(N_OEM, np.nan)

    return theta, theta_se, math.sqrt(sigma2), n_f


def stage_oem_joint_sigma(sorties_data, sigma0=None, max_iter=30,
                          window_s=5.0, verbose=False):
    """
    OEM with sigma as a free 7th parameter jointly estimated with the others.
    Gradient ∂cost/∂sigma = ∂cost/∂Ixx_ZFW * m_fuel(t) (from _oem_forward joint mode).

    Returns: theta7, theta_se7, resid_std_rads, n_total
      theta7 = [Ixx_ZFW, Cl_p, Cl_da, Cl_r, Cl_beta, Cl_dr, sigma_ft2]
    """
    from scipy.optimize import minimize as sp_minimize

    if sigma0 is None:
        sigma0 = SIGMA_DEFAULT

    # Seed the 6-param OEM at sigma0 to get a good starting point
    theta6, _, _, _ = stage_oem(sorties_data, sigma0,
                                max_iter=max(10, max_iter // 2),
                                window_s=window_s, verbose=False)
    if theta6 is None:
        return None, None, None, 0

    theta0 = np.append(theta6, sigma0)

    dt       = float(sorties_data[0].get("dt", 0.01))
    window_n = max(10, int(round(window_s / dt)))

    bounds = [
        (100.0, 60000.0),  # Ixx_ZFW
        (-20.0,   -0.05),  # Cl_p
        ( -2.0,    2.0),   # Cl_da
        ( -2.0,    2.0),   # Cl_r
        ( -2.0,    2.0),   # Cl_beta
        ( -2.0,    2.0),   # Cl_dr
        (  1.0,  680.0),   # sigma ft² (y from ~1 ft to full semi-span ~26 ft)
    ]

    scale = np.maximum(np.abs(theta0), 0.01)
    call_count = [0]

    def _cost_grad(theta_s):
        theta = theta_s * scale
        call_count[0] += 1
        JtJ, Jtr, cost, n = _oem_forward(sorties_data, theta, None, window_n, dt=dt)
        if n == 0:
            return 1e30, np.zeros(N_OEM_JS)
        grad_scaled = -2.0 * Jtr * scale
        rms = math.sqrt(cost / n)
        if verbose:
            print(f"  call {call_count[0]:3d}  "
                  f"rms={np.degrees(rms)*1000:.3f} m°/s  "
                  f"Ixx_ZFW={theta[0]:.0f}  Cl_p={theta[1]:.4f}  sigma={theta[6]:.1f}")
        return float(cost), grad_scaled

    bounds_s  = [(lo / sc, hi / sc) for (lo, hi), sc in zip(bounds, scale)]
    theta0_s  = theta0 / scale

    result = sp_minimize(
        _cost_grad, theta0_s,
        method="L-BFGS-B", jac=True,
        bounds=bounds_s,
        options={"maxiter": max_iter * 10, "ftol": 1e-15, "gtol": 1e-8,
                 "maxfun": max_iter * 50},
    )
    theta = result.x * scale

    if verbose:
        print(f"\n  L-BFGS-B: {result.message}")

    JtJ_f, _, cost_f, n_f = _oem_forward(sorties_data, theta, None, window_n, dt=dt)
    dof    = max(1, n_f - N_OEM_JS)
    sigma2 = cost_f / dof
    try:
        cov      = sigma2 * np.linalg.pinv(JtJ_f)
        theta_se = np.sqrt(np.abs(np.diag(cov)))
    except Exception:
        theta_se = np.full(N_OEM_JS, np.nan)

    return theta, theta_se, math.sqrt(sigma2), n_f


def sigma_scan_oem(sorties_data, sigma_grid, max_iter=30, window_s=5.0):
    """
    Run OEM for each sigma in sigma_grid, warm-starting from previous result.
    Returns list of (sigma, Ixx_ZFW, Cl_p, resid_std_rads, n_total).
    """
    n_total = len(sigma_grid)
    print(f"  {'#':>3}  {'sigma':>8}  {'Ixx_ZFW':>10}  {'Cl_p':>10}  {'resid m°/s':>12}",
          flush=True)
    print(f"  {'-'*50}", flush=True)

    results = []
    theta_warm = None
    for i, sigma in enumerate(sigma_grid):
        theta, _, resid, n = stage_oem(
            sorties_data, float(sigma), theta0=theta_warm,
            max_iter=max_iter, window_s=window_s, verbose=False,
        )
        if theta is None or resid is None:
            print(f"  {i+1:>3}/{n_total}  {sigma:>8.1f}  {'FAILED':>10}", flush=True)
            theta_warm = None
            continue
        resid_mdeg = np.degrees(resid) * 1000
        print(f"  {i+1:>3}/{n_total}  {sigma:>8.1f}  {theta[0]:>10.1f}  "
              f"{theta[1]:>10.4f}  {resid_mdeg:>12.4f}", flush=True)
        results.append((float(sigma), float(theta[0]), float(theta[1]), float(resid), n))
        theta_warm = theta.copy()
    return results


# ── Stage 1: hierarchical regression (roll) ────────────────────────────────────

N_GLOB = 4   # eta (Cl_da), mu_r (Cl_r), mu_beta (Cl_beta), mu_dr (Cl_dr), all /|Cl_p|


def _build_global_cols(qSb_m, da_m, rb2V_m, beta_m, dr_m):
    """Return (n_samples, 4) array of global regressors scaled by qSb."""
    return np.column_stack([
        -qSb_m * da_m,      # eta   = Cl_da / |Cl_p|
        -qSb_m * rb2V_m,    # mu_r  = Cl_r  / |Cl_p|
        -qSb_m * beta_m,    # mu_b  = Cl_beta / |Cl_p|
        -qSb_m * dr_m,      # mu_dr = Cl_dr / |Cl_p|
    ])


def stage1_hierarchical(sorties_data, verbose=False):
    """
    Solve the joint system across all sorties:
        y_j = -qSb_j * pb_2V_j
        Psi_i * p_dot_j + eta*(-qSb*da) + mu_r*(-qSb*rb/2V)
            + mu_beta*(-qSb*beta) + mu_dr*(-qSb*dr) = y_j
    for j in sortie i.

    Parameters (N_sorties + N_GLOB):
        theta = [Psi_0 ... Psi_{N-1}, eta, mu_r, mu_beta, mu_dr]

    Returns: theta, theta_se, residual_std, n_samples_per_sortie
    """
    N   = len(sorties_data)
    dim = N + N_GLOB
    XtX = np.zeros((dim, dim))
    Xty = np.zeros(dim)

    n_per_sortie = []
    for i, sd in enumerate(sorties_data):
        arr  = sd["arrays"]
        mask = arr["mask"]
        if mask.sum() < 50:
            n_per_sortie.append(0)
            continue

        p_dot_m = arr["p_dot"][mask]
        qSb_m   = arr["qSb"][mask]
        da_m    = arr["da_rad"][mask]
        pb2V_m  = arr["pb_2V"][mask]
        rb2V_m  = arr["rb_2V"][mask]
        beta_m  = arr["beta_rad"][mask]
        dr_m    = arr["dr_rad"][mask]

        ok = (np.isfinite(p_dot_m) & np.isfinite(qSb_m) & np.isfinite(da_m) &
              np.isfinite(pb2V_m)  & np.isfinite(rb2V_m) & np.isfinite(beta_m) &
              np.isfinite(dr_m))
        n_ok = int(ok.sum())
        n_per_sortie.append(n_ok)
        if n_ok < 50:
            continue

        p_dot_m = p_dot_m[ok]; qSb_m = qSb_m[ok]; da_m = da_m[ok]
        pb2V_m  = pb2V_m[ok];  rb2V_m = rb2V_m[ok]
        beta_m  = beta_m[ok];  dr_m   = dr_m[ok]

        y_m     = -qSb_m * pb2V_m
        G_m     = _build_global_cols(qSb_m, da_m, rb2V_m, beta_m, dr_m)  # (n,4)

        # Per-sortie diagonal
        XtX[i, i]          += float(np.dot(p_dot_m, p_dot_m))
        # Cross per-sortie <-> global (shape 4)
        cross = p_dot_m @ G_m                                              # (4,)
        XtX[i, N:N+N_GLOB] += cross
        XtX[N:N+N_GLOB, i] += cross
        # Global 4×4 block
        XtX[N:N+N_GLOB, N:N+N_GLOB] += G_m.T @ G_m
        # RHS
        Xty[i]             += float(np.dot(p_dot_m, y_m))
        Xty[N:N+N_GLOB]    += G_m.T @ y_m

    total_n = sum(n_per_sortie)
    if total_n < dim:
        return None, None, None, n_per_sortie

    try:
        theta = np.linalg.solve(XtX, Xty)
    except np.linalg.LinAlgError:
        theta = np.linalg.lstsq(XtX, Xty, rcond=None)[0]

    # Residuals
    ss_res  = 0.0
    n_total = 0
    for i, sd in enumerate(sorties_data):
        arr  = sd["arrays"]
        mask = arr["mask"]
        if mask.sum() < 50:
            continue
        p_dot_m = arr["p_dot"][mask]; qSb_m = arr["qSb"][mask]
        da_m    = arr["da_rad"][mask]; pb2V_m = arr["pb_2V"][mask]
        rb2V_m  = arr["rb_2V"][mask]; beta_m = arr["beta_rad"][mask]
        dr_m    = arr["dr_rad"][mask]
        ok = (np.isfinite(p_dot_m) & np.isfinite(qSb_m) & np.isfinite(da_m) &
              np.isfinite(pb2V_m)  & np.isfinite(rb2V_m) & np.isfinite(beta_m) &
              np.isfinite(dr_m))
        if ok.sum() < 50:
            continue
        p_dot_m = p_dot_m[ok]; qSb_m = qSb_m[ok]; da_m = da_m[ok]
        pb2V_m  = pb2V_m[ok];  rb2V_m = rb2V_m[ok]
        beta_m  = beta_m[ok];  dr_m   = dr_m[ok]
        y_m  = -qSb_m * pb2V_m
        G_m  = _build_global_cols(qSb_m, da_m, rb2V_m, beta_m, dr_m)
        pred = theta[i] * p_dot_m + G_m @ theta[N:N+N_GLOB]
        res  = y_m - pred
        ss_res  += float(np.dot(res, res))
        n_total += int(ok.sum())

    dof    = max(1, n_total - dim)
    sigma2 = ss_res / dof
    try:
        cov      = sigma2 * np.linalg.pinv(XtX)
        theta_se = np.sqrt(np.abs(np.diag(cov)))
    except Exception:
        theta_se = np.full(dim, np.nan)

    resid_std = math.sqrt(sigma2) if n_total > dim else float("nan")
    return theta, theta_se, resid_std, n_per_sortie


# ── Stage 2: fuel-state regression ────────────────────────────────────────────

def stage2_fuel_regression(Psi_vals, Psi_se, m_fuel_mean, sigma_ft2, verbose=False):
    """
    Fit: Psi_i = Ixx_ZFW/|Cl_p| + (sigma/|Cl_p|) * m_fuel_i

    Uses weighted LS (weights = 1/Psi_se^2) where Psi_se > 0.
    Returns: dict with Ixx_ZFW, Cl_p, slope, intercept, r2.
    """
    valid = (
        np.isfinite(Psi_vals) & np.isfinite(Psi_se) &
        np.isfinite(m_fuel_mean) & (Psi_se > 0) & (Psi_vals > 0)
    )
    if valid.sum() < 3:
        return None, "fewer than 3 valid sorties for Stage 2"

    Psi_v = Psi_vals[valid]
    m_v   = m_fuel_mean[valid]
    w     = 1.0 / (Psi_se[valid] ** 2)

    # Weighted LS: [1, m] . [intercept, slope] = Psi
    X2 = np.column_stack([np.ones(valid.sum()), m_v])
    W  = np.diag(w)
    XWX = X2.T @ W @ X2
    XWy = X2.T @ (w * Psi_v)
    try:
        coef = np.linalg.solve(XWX, XWy)
    except np.linalg.LinAlgError:
        coef = np.linalg.lstsq(X2, Psi_v, rcond=None)[0]

    intercept, slope = coef

    if slope <= 0:
        return None, f"Stage 2 slope={slope:.4g} <= 0 — fuel variation too small or wrong sign"

    Cl_p_abs = sigma_ft2 / slope          # |Cl_p| from geometry
    Ixx_ZFW  = intercept * Cl_p_abs

    # R^2
    pred     = intercept + slope * m_v
    ss_res   = float(np.sum(w * (Psi_v - pred) ** 2))
    ss_tot   = float(np.sum(w * (Psi_v - np.average(Psi_v, weights=w)) ** 2))
    r2       = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    # Standard errors on intercept and slope
    n2  = valid.sum()
    resid2 = Psi_v - pred
    sig2_2 = float(np.dot(resid2, resid2)) / max(1, n2 - 2)
    try:
        cov2 = sig2_2 * np.linalg.pinv(XWX)
        se_int, se_slope = np.sqrt(np.abs(np.diag(cov2)))
    except Exception:
        se_int = se_slope = float("nan")

    se_Cl_p_abs = Cl_p_abs * se_slope / slope  # propagated
    se_Ixx_ZFW  = math.sqrt((intercept * se_Cl_p_abs) ** 2 +
                             (Cl_p_abs  * se_int)      ** 2)

    return {
        "slope":        float(slope),
        "intercept":    float(intercept),
        "Cl_p_abs":     float(Cl_p_abs),
        "Cl_p":         float(-Cl_p_abs),
        "Ixx_ZFW":      float(Ixx_ZFW),
        "se_slope":     float(se_slope),
        "se_intercept": float(se_int),
        "se_Cl_p":      float(se_Cl_p_abs),
        "se_Ixx_ZFW":   float(se_Ixx_ZFW),
        "r2_stage2":    float(r2),
        "n_sorties":    int(valid.sum()),
    }, None


# ── Roll time-constant (tau) method ───────────────────────────────────────────

# Step detection parameters
_TAU_DA_STEP_DEG   = 3.0    # min aileron step magnitude (deg)
_TAU_DA_RATE_DPS   = 20.0   # min aileron rate (deg/s) — require sharp inputs only
_TAU_P_SS_MIN_DEG  = 3.0    # min steady-state |p| after step (deg/s)
_TAU_WIN_S         = 2.0    # window (s) after step for exponential fit
_TAU_MAX_QUAL      = 0.25   # max normalized RMSE (residual / |A|) — strict quality
_TAU_MIN_S         = 0.04   # min plausible time constant (40 ms)
_TAU_MAX_S         = 0.40   # max plausible time constant (400 ms at slow airspeed)
_TAU_MIN_SEP_S     = 2.5    # min separation between detected steps


def _fit_tau(t_rel, p_vals, p0):
    """Fit p(t)=p0+A*(1-exp(-t/tau)) to post-step data. Returns (tau, A, quality)."""
    def model(t, A, tau):
        return p0 + A * (1.0 - np.exp(-np.clip(t, 0.0, None) / tau))
    try:
        A_init = float(p_vals[-1] - p0)
        popt, _ = curve_fit(
            model, t_rel, p_vals,
            p0=[A_init, 0.2],
            bounds=([-np.inf, _TAU_MIN_S], [np.inf, _TAU_MAX_S]),
            maxfev=800,
        )
        A_fit, tau_fit = popt
        pred = model(t_rel, *popt)
        rmse = float(np.sqrt(np.mean((p_vals - pred) ** 2)))
        quality = rmse / max(abs(A_fit), 0.5)
        return tau_fit, A_fit, quality
    except Exception:
        return None, None, float("inf")


def detect_roll_maneuvers(afcs_data, verbose=False):
    """
    Find aileron step events and fit exponential roll responses.
    Returns list of dicts: {tau, qSb, V, m_fuel_slug, sortie, Psi_tau}.
    """
    required = {"p", "daL", "daR", "tas"}
    if not required.issubset(afcs_data.keys()):
        return []

    t_p, p_deg = afcs_data["p"]
    dt = float(np.median(np.diff(t_p)))
    if dt <= 0 or dt > 0.015:   # need ~100 Hz
        return []

    t_uni = t_p   # already approximately uniform

    def _interp(key):
        if key in afcs_data:
            return np.interp(t_uni, *afcs_data[key])
        return np.zeros(len(t_uni))

    tas     = _interp("tas")
    h_ft    = _interp("press_alt")
    daL     = _interp("daL")
    daR     = _interp("daR")
    mL      = _interp("mL")
    mR      = _interp("mR")

    da = 0.5 * (daL - daR)   # signed aileron position, deg
    T_R = np.clip(518.67 - 3.5662e-3 * h_ft, 389.97, 518.67)
    rho = RHO_SL * (T_R / 518.67) ** 4.2561
    q   = 0.5 * rho * tas ** 2
    qSb = q * S_FT2 * B_FT

    # Light smoothing of p and da for step detection
    win5 = 5
    p_sm  = savgol_filter(p_deg, win5, 3)
    da_sm = savgol_filter(da,    win5, 3)
    da_rate = np.gradient(da_sm, dt)   # deg/s

    airborne = (tas > 50 * KTS2FPS) & np.isfinite(da) & np.isfinite(p_sm)
    min_sep_n = int(_TAU_MIN_SEP_S / dt)
    win_n     = int(_TAU_WIN_S / dt)

    # Find step onset candidates
    step_flag = airborne & (np.abs(da_rate) > _TAU_DA_RATE_DPS)
    # cluster into events
    idxs = np.where(step_flag)[0]
    events = []
    if len(idxs) == 0:
        return []

    clusters = [[idxs[0]]]
    for idx in idxs[1:]:
        if idx - clusters[-1][-1] < int(0.5 / dt):
            clusters[-1].append(idx)
        else:
            clusters.append([idx])

    last_event_idx = -min_sep_n

    for cl in clusters:
        cl_arr = np.array(cl)
        i0 = int(cl_arr[np.argmax(np.abs(da_rate[cl_arr]))])

        if i0 - last_event_idx < min_sep_n:
            continue
        if not airborne[i0]:
            continue

        # Confirm step magnitude: |da change over the cluster|
        i_before = max(0, i0 - int(0.3 / dt))
        i_after  = min(len(da_sm) - 1, i0 + int(0.3 / dt))
        da_step  = float(da_sm[i_after] - da_sm[i_before])
        if abs(da_step) < _TAU_DA_STEP_DEG:
            continue

        # Steady-state roll rate: average from 1.5s to 2.5s after step
        i_ss_start = min(len(t_uni) - 1, i0 + int(1.5 / dt))
        i_ss_end   = min(len(t_uni) - 1, i0 + int(2.5 / dt))
        if i_ss_end <= i_ss_start:
            continue
        p_ss = float(np.median(p_sm[i_ss_start:i_ss_end]))

        if abs(p_ss) < _TAU_P_SS_MIN_DEG:
            continue

        # Pre-step baseline
        i_pre = max(0, i0 - int(0.3 / dt))
        p0 = float(np.mean(p_sm[i_pre:i0])) if i0 > i_pre else float(p_sm[i0])

        # Fit window
        i_end_fit = min(len(t_uni) - 1, i0 + win_n)
        if i_end_fit - i0 < int(0.5 / dt):
            continue

        t_rel  = t_uni[i0:i_end_fit] - t_uni[i0]
        p_vals = p_sm[i0:i_end_fit]

        tau, A_fit, quality = _fit_tau(t_rel, p_vals, p0)
        if tau is None or quality > _TAU_MAX_QUAL:
            continue
        if not (_TAU_MIN_S <= tau <= _TAU_MAX_S):
            continue

        # Flight condition at step
        V_step   = float(np.nanmean(tas[i0:min(len(tas), i0 + int(1.0 / dt))]))
        qSb_step = float(np.nanmean(qSb[i0:min(len(qSb), i0 + int(1.0 / dt))]))
        if V_step < 50 or qSb_step < 1e4:
            continue

        m_fuel = float((np.nanmean(mL[i0:min(len(mL), i0 + int(1.0 / dt))]) +
                        np.nanmean(mR[i0:min(len(mR), i0 + int(1.0 / dt))])) / G_FPS2)

        Psi_tau = tau * qSb_step * B_FT / (2.0 * V_step)

        events.append({
            "tau":         tau,
            "qSb":         qSb_step,
            "V":           V_step,
            "m_fuel_slug": m_fuel,
            "Psi_tau":     Psi_tau,
            "da_step":     da_step,
            "p_ss":        p_ss,
            "quality":     quality,
        })
        last_event_idx = i0

    return events


def stage2_tau(all_events, sigma_ft2, verbose=False):
    """
    Fit Psi_tau_k = intercept + slope * m_fuel_k across all events,
    weighted by event quality (lower normalized RMSE = higher weight).

    Returns same dict shape as stage2_fuel_regression.
    """
    if len(all_events) < 5:
        return None, "fewer than 5 total maneuver events"

    Psi_v  = np.array([e["Psi_tau"]     for e in all_events])
    m_v    = np.array([e["m_fuel_slug"] for e in all_events])
    qual_v = np.array([e["quality"]     for e in all_events])   # lower = better

    # Weight inversely by quality (normalized RMSE)
    w = 1.0 / np.clip(qual_v ** 2, 1e-6, None)
    w /= w.mean()

    valid = np.isfinite(Psi_v) & np.isfinite(m_v) & (Psi_v > 0)
    if valid.sum() < 5:
        return None, "fewer than 5 valid events"

    Psi_v = Psi_v[valid]; m_v = m_v[valid]; w = w[valid]

    X2  = np.column_stack([np.ones(len(Psi_v)), m_v])
    W   = np.diag(w)
    XWX = X2.T @ W @ X2
    XWy = X2.T @ (w * Psi_v)
    try:
        coef = np.linalg.solve(XWX, XWy)
    except np.linalg.LinAlgError:
        coef = np.linalg.lstsq(X2, Psi_v, rcond=None)[0]

    intercept, slope = coef
    if slope <= 0:
        return None, f"tau Stage 2 slope={slope:.4g} <= 0"

    Cl_p_abs = sigma_ft2 / slope
    Ixx_ZFW  = intercept * Cl_p_abs

    pred   = intercept + slope * m_v
    ss_res = float(np.sum(w * (Psi_v - pred) ** 2))
    ss_tot = float(np.sum(w * (Psi_v - np.average(Psi_v, weights=w)) ** 2))
    r2     = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    n2     = int(valid.sum())
    resid2 = Psi_v - pred
    sig2_2 = float(np.dot(resid2, resid2)) / max(1, n2 - 2)
    try:
        cov2              = sig2_2 * np.linalg.pinv(XWX)
        se_int, se_slope  = np.sqrt(np.abs(np.diag(cov2)))
    except Exception:
        se_int = se_slope = float("nan")

    se_Cl_p = Cl_p_abs * se_slope / slope
    se_Ixx  = math.sqrt((intercept * se_Cl_p) ** 2 + (Cl_p_abs * se_int) ** 2)

    return {
        "slope":        float(slope),
        "intercept":    float(intercept),
        "Cl_p_abs":     float(Cl_p_abs),
        "Cl_p":         float(-Cl_p_abs),
        "Ixx_ZFW":      float(Ixx_ZFW),
        "se_slope":     float(se_slope),
        "se_intercept": float(se_int),
        "se_Cl_p":      float(se_Cl_p),
        "se_Ixx_ZFW":   float(se_Ixx),
        "r2_stage2":    float(r2),
        "n_events":     n2,
    }, None


# ── Main ───────────────────────────────────────────────────────────────────────

def find_sortie_dirs(root, sortie_filter=None):
    """Find all N208B sortie directories that have an AFCS cache file."""
    pattern = os.path.join(root, "*_N208B", AFCS_CACHE_FILE)
    paths   = sorted(glob.glob(pattern))
    dirs    = [os.path.dirname(p) for p in paths]
    if sortie_filter:
        dirs = [d for d in dirs if sortie_filter in os.path.basename(d)]
    return dirs


def load_sortie(sortie_dir, verbose=False):
    """
    Load one sortie. Returns (data_dict, reason); None on rejection.
    All signals at 100 Hz from .afcs_signals.npz (extract_afcs.py v2+).
    """
    sortie = os.path.basename(sortie_dir)

    if sortie.startswith("G"):
        return None, f"{sortie}: ground sortie"

    afcs_data = _load_afcs(sortie_dir)
    if afcs_data is None:
        return None, f"{sortie}: missing .afcs_signals.npz — run extract_afcs.py first"
    missing = REQUIRED_SIGS - set(afcs_data.keys())
    if missing:
        return None, f"{sortie}: AFCS cache missing {sorted(missing)}"

    t_p, _ = afcs_data["p"]
    dt_hi   = float(np.median(np.diff(t_p)))
    if dt_hi <= 0 or dt_hi > 0.05:
        return None, f"{sortie}: unexpected roll_rate dt={dt_hi:.4f}s"

    t_uni = np.arange(t_p[0], t_p[-1], dt_hi)
    if len(t_uni) < 1000:
        return None, f"{sortie}: < 1000 samples"

    grid = {key: _to_grid(t_uni, t, y) for key, (t, y) in afcs_data.items()}

    wt       = grid.get("wt",   np.full(len(t_uni), np.nan))
    fuel     = grid.get("fuel", np.full(len(t_uni), np.nan))
    wt_max   = np.nanmax(wt)
    fuel_max = np.nanmax(fuel)
    if wt_max - fuel_max < 100.0:
        return None, (f"{sortie}: WT_current ~= total_fuel "
                      f"({wt_max:.0f} vs {fuel_max:.0f} lb) — FCC ZFW not initialized")

    tas = grid["tas"]   # ft/s
    if np.nanmax(tas) < 50 * KTS2FPS:   # 50 kts = 84 ft/s
        return None, f"{sortie}: TAS never > 50 kts"

    return {"sortie": sortie, "t": t_uni, "grid": grid, "dt": dt_hi}, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sigma",      type=float, default=None,
                    help="Override fuel tank σ=∫y²dm/m (ft²) directly. "
                         "If omitted, computed from --tank-y1/--tank-y2 or defaults to "
                         f"{SIGMA_DEFAULT:.0f} ft² (y_tank={Y_TANK_DEFAULT:.1f} ft).")
    ap.add_argument("--tank-y1",   type=float, default=None,
                    help="Inner fuel tank span station (ft from CL). "
                         "Used with --tank-y2 to compute σ for a uniform tank distribution.")
    ap.add_argument("--tank-y2",   type=float, default=None,
                    help="Outer fuel tank span station (ft from CL).")
    ap.add_argument("--sortie",     default=None, help="Limit to one sortie substring")
    ap.add_argument("--sg-window",  type=float, default=0.3,
                    help="SavGol window for p_dot (s). Default 0.3")
    ap.add_argument("--direct",      action="store_true",
                    help="Single-stage global regression: Ixx_ZFW and Cl_p estimated jointly "
                         "using time-varying m_fuel(t) as known RHS correction (no Stage 2)")
    ap.add_argument("--tau-method",  action="store_true",
                    help="Use roll time-constant estimation instead of p_dot regression")
    ap.add_argument("--pitch-mode", action="store_true",
                    help="Estimate Iyy/|Cm_q| from pitch equation instead of Ixx")
    ap.add_argument("--cm-q",       type=float, default=None,
                    help="Supply |Cm_q| to compute absolute Iyy (e.g. 10.0)")
    ap.add_argument("--min-pdot-rms", type=float, default=0.0,
                    help="Minimum per-sortie p_dot RMS (deg/s²) for Stage 2. "
                         "Filters out low-excitation sorties that suffer more collinearity bias.")
    ap.add_argument("--oem",         action="store_true",
                    help="Output Error Method (JIO): minimise ||p_meas-p_pred||² "
                         "via Gauss-Newton, seeded from equation-error global regression.")
    ap.add_argument("--oem-window",  type=float, default=5.0,
                    help="OEM integration window length (s). Default 5.0")
    ap.add_argument("--oem-iter",    type=int,   default=30,
                    help="Max Gauss-Newton iterations. Default 30")
    ap.add_argument("--sigma-scan",     action="store_true",
                    help="Scan sigma over a grid, run OEM at each, report residual vs sigma.")
    ap.add_argument("--sigma-scan-min", type=float, default=10.0,
                    help="Minimum sigma for scan (ft²). Default 10.")
    ap.add_argument("--sigma-scan-max", type=float, default=200.0,
                    help="Maximum sigma for scan (ft²). Default 200.")
    ap.add_argument("--sigma-scan-n",   type=int,   default=20,
                    help="Number of scan points. Default 20.")
    ap.add_argument("--sigma-joint",    action="store_true",
                    help="Jointly estimate sigma as a free 7th parameter in OEM.")
    ap.add_argument("--out",        default="inertia_results.json")
    ap.add_argument("--verbose",    action="store_true")
    args = ap.parse_args()

    # ── sigma resolution ───────────────────────────────────────────────────────
    if args.sigma is not None:
        sigma  = args.sigma
        y_tank = math.sqrt(sigma)
        sigma_src = f"--sigma override"
    elif args.tank_y1 is not None and args.tank_y2 is not None:
        y1, y2 = sorted([args.tank_y1, args.tank_y2])
        sigma  = sigma_from_tank_bounds(y1, y2)
        y_tank = math.sqrt(sigma)
        sigma_src = f"uniform tank y={y1:.1f}→{y2:.1f} ft"
    else:
        sigma  = SIGMA_DEFAULT
        y_tank = Y_TANK_DEFAULT
        sigma_src = f"default point-mass at y_tank={Y_TANK_DEFAULT:.1f} ft"

    print(f"sigma = {sigma:.2f} ft²  ({sigma_src})")
    print(f"  → equivalent point-mass arm y_eff = {y_tank:.2f} ft")
    print()
    print("  Sigma sensitivity (Ixx ∝ sigma in equation-error methods):")
    print(f"  {'Tank bounds':>22}  {'sigma':>8}  {'y_eff':>7}")
    print(f"  {'-'*45}")
    for y1c, y2c in [(2,9), (2,12), (3,15), (3,20)]:
        sc = sigma_from_tank_bounds(y1c, y2c)
        print(f"  {'y='+str(y1c)+'→'+str(y2c)+' ft (uniform)':>22}  {sc:>8.1f}  {math.sqrt(sc):>7.2f}")
    print(f"  {'y_tank=7 ft (point, default)':>22}  {49:>8.1f}  {7.0:>7.2f}")
    print()

    dirs = find_sortie_dirs(ROOT, args.sortie)
    print(f"Found {len(dirs)} sortie(s) with AFCS cache")

    accepted  = []
    rejected  = []

    # Load all sorties
    for sortie_dir in dirs:
        data, reason = load_sortie(sortie_dir, verbose=args.verbose)
        if data is None:
            rejected.append({"sortie": os.path.basename(sortie_dir),
                              "reason": reason})
            if args.verbose:
                print(f"  SKIP  {reason}")
            continue

        arr = build_sortie_arrays(data, sg_window_s=args.sg_window)
        n_ok = int(arr["mask"].sum())
        if n_ok < 50:
            rejected.append({"sortie": data["sortie"],
                              "reason": f"only {n_ok} valid samples after masking"})
            continue

        g = data["grid"]
        m_fuel = arr["m_fuel_slug"][arr["mask"]]
        wt_arr = arr["wt_lb"][arr["mask"]]
        m_fuel_mean = float(np.nanmean(m_fuel)) * G_FPS2  # back to lb for reporting

        # Build pitch arrays if pitch mode requested and signals available
        pitch_arr = None
        if args.pitch_mode:
            missing_p = REQUIRED_SIGS_PITCH - set(g.keys())
            if not missing_p:
                pitch_arr = build_pitch_arrays(data, sg_window_s=args.sg_window)

        accepted.append({
            "sortie":        data["sortie"],
            "dt":            data["dt"],
            "n_samples":     n_ok,
            "fuel_mean_lb":  m_fuel_mean,
            "fuel_range_lb": [float(np.nanmin(g["fuel"])), float(np.nanmax(g["fuel"]))],
            "wt_mean_lb":    float(np.nanmean(wt_arr)),
            "pdot_rms_dps":  arr["pdot_rms_dps"],
            "arrays":        arr,
            "arrays_pitch":  pitch_arr,
        })
        print(f"  OK  {data['sortie']:25s}  n={n_ok:>7,}  fuel={m_fuel_mean:.0f} lb  "
              f"pdot_rms={arr['pdot_rms_dps']:.2f} °/s²")

    oem_mode = args.oem or args.sigma_scan or args.sigma_joint
    min_sorties = 1 if oem_mode else 3
    if len(accepted) < min_sorties:
        label = "OEM" if oem_mode else "Stage 2"
        print(f"\nOnly {len(accepted)} sorties loaded — need at least {min_sorties} for {label}.")
        sys.exit(1)

    # ── Pitch mode ─────────────────────────────────────────────────────────────
    if args.pitch_mode:
        print(f"\n{'='*55}")
        print(f"Pitch axis: Iyy/|Cm_q| estimation  ({len(accepted)} sorties)")
        print(f"  c_bar = {C_BAR_FT:.3f} ft  (S/b)")
        print(f"{'='*55}")

        # Filter to sorties with valid pitch arrays (built during loading)
        pitch_accepted = [sd for sd in accepted if sd.get("arrays_pitch") is not None
                          and sd["arrays_pitch"]["mask"].sum() >= 50]
        print(f"{len(pitch_accepted)} sorties with valid pitch data")

        theta_p, theta_se_p, resid_p, n_per_p = stage1_pitch(pitch_accepted, verbose=args.verbose)
        if theta_p is None:
            print("Pitch Stage 1 failed.")
            sys.exit(1)

        Np    = len(pitch_accepted)
        Psi_p = theta_p[:Np]
        xi    = float(theta_p[Np])
        Psi_p_se = theta_se_p[:Np] if theta_se_p is not None else np.full(Np, np.nan)
        xi_se    = float(theta_se_p[Np]) if theta_se_p is not None else float("nan")

        print(f"\n  xi(Cm_de/|Cm_q|) = {xi:+.5g}  +/-2se={2*xi_se:.3g}")
        print(f"  residual std     = {resid_p:.4g} ft.lb")
        print()
        print(f"Per-sortie Psi_pitch = Iyy/|Cm_q|:")
        m_fuel_arr_p = np.array([sd["fuel_mean_lb"] for sd in pitch_accepted])

        sep = "-" * 65
        print(sep)
        print(f"  {'Sortie':<25}  {'Psi_pitch':>10}  {'+/-se':>8}  {'fuel_lb':>8}  {'SNR':>6}")
        print(sep)
        for i, sd in enumerate(pitch_accepted):
            ok = (n_per_p[i] >= 50) if i < len(n_per_p) else False
            snr = abs(Psi_p[i]) / Psi_p_se[i] if Psi_p_se[i] > 0 else float("nan")
            psi_str = f"{Psi_p[i]:.1f}" if ok else "skip"
            se_str  = f"{Psi_p_se[i]:.1f}" if ok else "---"
            print(f"  {sd['sortie']:<25}  {psi_str:>10}  {se_str:>8}  "
                  f"{sd['fuel_mean_lb']:>8.0f}  {snr:>6.1f}")

        # Optional: report Iyy if user supplied Cm_q
        print(f"\n{'='*55}")
        if args.cm_q is not None:
            Cm_q_abs = abs(args.cm_q)
            print(f"Iyy estimates (using |Cm_q| = {Cm_q_abs:.2f} from --cm-q):")
            print(sep)
            for i, sd in enumerate(pitch_accepted):
                Iyy_i = Psi_p[i] * Cm_q_abs
                print(f"  {sd['sortie']:<25}  Iyy={Iyy_i:>10.0f} slug.ft2  "
                      f"fuel={sd['fuel_mean_lb']:.0f} lb")
        else:
            print("To get absolute Iyy, supply --cm-q <value> (e.g. --cm-q 10.0)")
            Cm_da_from_xi = xi   # Cm_de / |Cm_q|
            print(f"  xi = Cm_de/|Cm_q| = {xi:+.4f}")
            print(f"  Typical Cm_q for Caravan class: -5 to -15 /rad")
            print(f"  => Cm_de ≈ xi × |Cm_q| = {xi:+.4f} × (5 to 15) "
                  f"= {xi*5:+.2f} to {xi*15:+.2f} /rad")
            Psi_p_med = float(np.nanmedian(Psi_p))
            print(f"  Median Psi_pitch = {Psi_p_med:.0f} slug.ft2")
            print(f"  => Iyy (if |Cm_q|=10): {Psi_p_med * 10:.0f} slug.ft2")
            print(f"  => Iyy (if |Cm_q|= 5): {Psi_p_med * 5:.0f} slug.ft2")
            print(f"  => Iyy (if |Cm_q|=15): {Psi_p_med * 15:.0f} slug.ft2")

        # Save
        out_path = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
        output_p = {
            "method":       "pitch",
            "c_bar_ft":     C_BAR_FT,
            "xi":           float(xi),
            "xi_se":        float(xi_se),
            "stage1_resid_std": resid_p,
            "n_sorties":    Np,
            "cm_q_supplied": args.cm_q,
            "sorties": [
                {
                    "sortie":      sd["sortie"],
                    "Psi_pitch":   float(Psi_p[i]),
                    "Psi_pitch_se":float(Psi_p_se[i]),
                    "fuel_mean_lb":sd["fuel_mean_lb"],
                    "Iyy":         float(Psi_p[i] * abs(args.cm_q)) if args.cm_q else None,
                }
                for i, sd in enumerate(pitch_accepted)
            ],
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output_p, f, indent=2)
        print(f"\nResults saved to {out_path}")
        return

    # ── Tau method ─────────────────────────────────────────────────────────────
    if args.tau_method:
        print(f"\n{'='*55}")
        print(f"Roll time-constant method  ({len(accepted)} sorties)")
        print(f"{'='*55}")
        print("Detecting aileron step maneuvers...")

        all_events = []
        event_sortie = []
        # Build a map from sortie name to directory path
        _sortie_dir_map = {os.path.basename(d): d for d in find_sortie_dirs(ROOT)}

        for sd in accepted:
            sortie_dir = _sortie_dir_map.get(sd["sortie"])
            if sortie_dir is None:
                continue
            afcs_data = _load_afcs(sortie_dir)
            if afcs_data is None:
                continue
            evs = detect_roll_maneuvers(afcs_data, verbose=args.verbose)
            if evs:
                for e in evs:
                    e["sortie"] = sd["sortie"]
                all_events.extend(evs)
                event_sortie.extend([sd["sortie"]] * len(evs))
            if args.verbose or True:
                print(f"  {sd['sortie']:25s}  {len(evs):>3} events  "
                      f"fuel={sd['fuel_mean_lb']:.0f} lb")

        print(f"\nTotal maneuver events: {len(all_events)}")
        if len(all_events) >= 5:
            qs = [e["quality"] for e in all_events]
            taus = [e["tau"] * 1000 for e in all_events]
            psis = [e["Psi_tau"] for e in all_events]
            print(f"  tau range: {min(taus):.0f}–{max(taus):.0f} ms  "
                  f"median={float(np.median(taus)):.0f} ms")
            print(f"  Psi range: {min(psis):.0f}–{max(psis):.0f}  "
                  f"median={float(np.median(psis)):.0f}")
            print(f"  quality (norm RMSE) median={float(np.median(qs)):.3f}")

        result, err = stage2_tau(all_events, sigma, verbose=args.verbose)
        if result is None:
            print(f"Tau Stage 2 failed: {err}")
            sys.exit(1)

        Cl_p_abs = result["Cl_p_abs"]
        Ixx_ZFW  = result["Ixx_ZFW"]
        Cl_p     = -Cl_p_abs
        y_tank   = math.sqrt(sigma)

        sep = "-" * 55
        print(f"\n{sep}")
        print(f"{'Parameter':<18}  {'Estimate':>12}  {'+/-2se':>10}  {'Unit'}")
        print(sep)
        print(f"{'Ixx_ZFW':<18}  {Ixx_ZFW:>12.1f}  {2*result['se_Ixx_ZFW']:>10.1f}  slug.ft2")
        print(f"{'Cl_p':<18}  {Cl_p:>12.5f}  {2*result['se_Cl_p']:>10.5f}  1/rad")
        print(f"{'sigma (y_tank^2)':<18}  {sigma:>12.1f}  {'---':>10}  ft^2")
        print(f"{'y_tank':<18}  {y_tank:>12.3f}  {'---':>10}  ft")
        print(sep)
        print(f"Stage 2 (tau)  R^2 = {result['r2_stage2']:.4f}  "
              f"({result['n_events']} events from {len(accepted)} sorties)")

        m_fuel_arr  = np.array([sd["fuel_mean_lb"] for sd in accepted])
        mean_fuel_lb   = float(np.mean(m_fuel_arr))
        mean_fuel_slug = mean_fuel_lb / G_FPS2
        Ixx_at_mean    = Ixx_ZFW + sigma * mean_fuel_slug
        print(f"Ixx_total at mean fuel ({mean_fuel_lb:.0f} lb) = {Ixx_at_mean:.1f} slug.ft2")

        out_path = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
        output = {
            "method":           "tau",
            "sigma_ft2":        sigma,
            "y_tank_ft":        y_tank,
            "Ixx_ZFW":          Ixx_ZFW,
            "se_Ixx_ZFW":       result["se_Ixx_ZFW"],
            "Cl_p":             Cl_p,
            "se_Cl_p":          result["se_Cl_p"],
            "stage2_r2":        result["r2_stage2"],
            "n_events":         result["n_events"],
            "n_sorties":        len(accepted),
            "rejected":         rejected,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {out_path}")
        return

    # Optional excitation filter — removes sorties with low p_dot RMS
    if args.min_pdot_rms > 0:
        n_before = len(accepted)
        low_exc  = [sd for sd in accepted if sd["pdot_rms_dps"] < args.min_pdot_rms]
        accepted = [sd for sd in accepted if sd["pdot_rms_dps"] >= args.min_pdot_rms]
        for sd in low_exc:
            rejected.append({"sortie": sd["sortie"],
                              "reason": f"pdot_rms={sd['pdot_rms_dps']:.2f} < --min-pdot-rms={args.min_pdot_rms:.2f}"})
        print(f"\nExcitation filter (--min-pdot-rms={args.min_pdot_rms:.2f}): "
              f"kept {len(accepted)}/{n_before} sorties")

    # ── OEM / JIO mode ─────────────────────────────────────────────────────────
    if args.oem:
        print(f"\n{'='*65}")
        print(f"OEM / JIO  ({len(accepted)} sorties,  window={args.oem_window:.1f} s,  "
              f"max_iter={args.oem_iter})")
        print(f"  Ixx(t) = Ixx_ZFW + sigma·m_fuel(t)  [sigma={sigma:.1f} ft²]")
        print(f"{'='*65}")

        theta_oem, se_oem, resid_oem, n_oem = stage_oem(
            accepted, sigma,
            max_iter=args.oem_iter,
            window_s=args.oem_window,
            verbose=args.verbose,
        )
        if theta_oem is None:
            print("OEM failed — check data or try --direct first.")
            sys.exit(1)

        Ixx_ZFW_oem = float(theta_oem[0])
        Cl_p_oem    = float(theta_oem[1])
        Cl_da_oem   = float(theta_oem[2])
        Cl_r_oem    = float(theta_oem[3])
        Cl_b_oem    = float(theta_oem[4])
        Cl_dr_oem   = float(theta_oem[5])

        m_fuel_arr    = np.array([sd["fuel_mean_lb"] for sd in accepted])
        mean_fuel_lb  = float(np.mean(m_fuel_arr))
        Ixx_at_mean   = Ixx_ZFW_oem + sigma * (mean_fuel_lb / G_FPS2)

        sep = "-" * 65
        print(f"\n{sep}")
        print(f"  {'Parameter':<20}  {'Estimate':>12}  {'+/-2se':>10}  {'Unit'}")
        print(sep)
        rows = [
            ("Ixx_ZFW",  Ixx_ZFW_oem, se_oem[0], "slug.ft2"),
            ("Cl_p",     Cl_p_oem,    se_oem[1], "/rad"),
            ("Cl_da",    Cl_da_oem,   se_oem[2], "/rad"),
            ("Cl_r",     Cl_r_oem,    se_oem[3], "/rad"),
            ("Cl_beta",  Cl_b_oem,    se_oem[4], "/rad"),
            ("Cl_dr",    Cl_dr_oem,   se_oem[5], "/rad"),
        ]
        for name, val, se, unit in rows:
            print(f"  {name:<20}  {val:>12.5g}  {2*se:>10.4g}  {unit}")
        print(sep)
        print(f"  n_total  = {n_oem:,}")
        print(f"  resid std = {np.degrees(resid_oem)*1000:.2f} m°/s  "
              f"({np.degrees(resid_oem):.4f} °/s)")
        print(f"  Ixx at mean fuel ({mean_fuel_lb:.0f} lb) = {Ixx_at_mean:.1f} slug.ft2")

        out_path = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
        output_oem = {
            "method":             "oem",
            "sigma_ft2":          sigma,
            "y_tank_ft":          y_tank,
            "window_s":           args.oem_window,
            "n_iter":             args.oem_iter,
            "Ixx_ZFW":            Ixx_ZFW_oem,
            "se_Ixx_ZFW":         float(se_oem[0]),
            "Cl_p":               Cl_p_oem,
            "se_Cl_p":            float(se_oem[1]),
            "Cl_da":              Cl_da_oem,
            "Cl_r":               Cl_r_oem,
            "Cl_beta":            Cl_b_oem,
            "Cl_dr":              Cl_dr_oem,
            "resid_std_rads":     float(resid_oem),
            "resid_std_degs":     float(np.degrees(resid_oem)),
            "n_total":            n_oem,
            "n_sorties":          len(accepted),
            "mean_fuel_lb":       mean_fuel_lb,
            "Ixx_at_mean_fuel":   float(Ixx_at_mean),
            "rejected":           rejected,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output_oem, f, indent=2)
        print(f"\nResults saved to {out_path}")
        return

    # ── Sigma scan ─────────────────────────────────────────────────────────────
    if args.sigma_scan:
        sigma_grid = np.linspace(args.sigma_scan_min, args.sigma_scan_max,
                                 args.sigma_scan_n)
        print(f"\n{'='*65}")
        print(f"Sigma scan  ({len(accepted)} sorties,  "
              f"{args.sigma_scan_n} points  "
              f"σ={args.sigma_scan_min:.0f}→{args.sigma_scan_max:.0f} ft²,  "
              f"window={args.oem_window:.1f} s,  max_iter={args.oem_iter})")
        print(f"{'='*65}")

        scan_results = sigma_scan_oem(
            accepted, sigma_grid,
            max_iter=args.oem_iter,
            window_s=args.oem_window,
        )

        if not scan_results:
            print("Sigma scan: no results.")
        else:
            resids = [r[3] for r in scan_results]
            best_idx = int(np.argmin(resids))
            sep = "-" * 65
            print(f"\n  {'sigma':>8}  {'Ixx_ZFW':>10}  {'Cl_p':>10}  "
                  f"{'resid m°/s':>12}  {'Ixx@mean_fuel':>14}")
            print(f"  {sep}")
            m_fuel_arr   = np.array([sd["fuel_mean_lb"] for sd in accepted])
            mean_fuel_lb = float(np.mean(m_fuel_arr))
            for i, (sig, Ixx0, Clp, resid, _) in enumerate(scan_results):
                Ixx_mean = Ixx0 + sig * (mean_fuel_lb / G_FPS2)
                marker = "  <-- min" if i == best_idx else ""
                print(f"  {sig:>8.1f}  {Ixx0:>10.1f}  {Clp:>10.4f}  "
                      f"{np.degrees(resid)*1000:>12.4f}  {Ixx_mean:>14.1f}{marker}")
            print(f"\n  Best sigma = {scan_results[best_idx][0]:.1f} ft²  "
                  f"(resid {np.degrees(scan_results[best_idx][3])*1000:.4f} m°/s)")
        return

    # ── Sigma joint ────────────────────────────────────────────────────────────
    if args.sigma_joint:
        print(f"\n{'='*65}")
        print(f"OEM joint-sigma  ({len(accepted)} sorties,  "
              f"window={args.oem_window:.1f} s,  max_iter={args.oem_iter})")
        print(f"  Ixx(t) = Ixx_ZFW + sigma·m_fuel(t)  [sigma FREE]")
        print(f"  seed sigma = {sigma:.1f} ft²")
        print(f"{'='*65}")

        theta7, se7, resid_js, n_js = stage_oem_joint_sigma(
            accepted, sigma0=sigma,
            max_iter=args.oem_iter,
            window_s=args.oem_window,
            verbose=args.verbose,
        )
        if theta7 is None:
            print("Joint-sigma OEM failed.")
            sys.exit(1)

        Ixx_ZFW_js = float(theta7[0])
        Cl_p_js    = float(theta7[1])
        sigma_js   = float(theta7[6])
        y_eff_js   = math.sqrt(abs(sigma_js))

        m_fuel_arr   = np.array([sd["fuel_mean_lb"] for sd in accepted])
        mean_fuel_lb = float(np.mean(m_fuel_arr))
        Ixx_at_mean  = Ixx_ZFW_js + sigma_js * (mean_fuel_lb / G_FPS2)

        sep = "-" * 65
        print(f"\n{sep}")
        names_js = ["Ixx_ZFW", "Cl_p", "Cl_da", "Cl_r", "Cl_beta", "Cl_dr", "sigma"]
        units_js = ["slug.ft2", "/rad", "/rad", "/rad", "/rad", "/rad", "ft2"]
        print(f"  {'Parameter':<20}  {'Estimate':>12}  {'+/-2se':>10}  {'Unit'}")
        print(sep)
        for nm, val, se, unit in zip(names_js, theta7, se7, units_js):
            print(f"  {nm:<20}  {val:>12.5g}  {2*se:>10.4g}  {unit}")
        print(sep)
        print(f"  n_total  = {n_js:,}")
        print(f"  resid std = {np.degrees(resid_js)*1000:.2f} m°/s  "
              f"({np.degrees(resid_js):.4f} °/s)")
        print(f"  sigma_est = {sigma_js:.2f} ft²  (y_eff = {y_eff_js:.2f} ft)")
        print(f"  Ixx at mean fuel ({mean_fuel_lb:.0f} lb) = {Ixx_at_mean:.1f} slug.ft2")
        return

    # ── Direct mode ────────────────────────────────────────────────────────────
    if args.direct:
        print(f"\n{'='*60}")
        print(f"Direct global regression  ({len(accepted)} sorties)")
        print(f"  Ixx(t) = Ixx_ZFW + sigma·m_fuel(t)  [sigma={sigma:.1f} ft²]")
        print(f"{'='*60}")

        theta_d, theta_se_d, resid_d, n_d = stage1_global(accepted, sigma, verbose=args.verbose)
        if theta_d is None:
            print("Global regression failed — singular system.")
            sys.exit(1)

        Psi_ZFW_d = float(theta_d[0])
        slope_d   = float(theta_d[1])   # sigma / |Cl_p|
        eta_d     = float(theta_d[2])
        se_PZW_d  = float(theta_se_d[0])
        se_slope_d = float(theta_se_d[1])

        if slope_d <= 0:
            print(f"Global regression: slope = {slope_d:.4g} <= 0 — not physical.")
            sys.exit(1)

        Cl_p_abs_d = sigma / slope_d
        Ixx_ZFW_d  = Psi_ZFW_d * Cl_p_abs_d
        Cl_da_d    = eta_d * Cl_p_abs_d
        # Error propagation: Ixx_ZFW = Psi_ZFW × sigma/slope
        se_Ixx_d = math.sqrt(
            (Cl_p_abs_d * se_PZW_d) ** 2 +
            (Psi_ZFW_d * sigma / slope_d ** 2 * se_slope_d) ** 2
        )

        sep = "-" * 65
        print(f"\n{sep}")
        print(f"{'Parameter':<22}  {'Estimate':>12}  {'+/-2se':>12}  {'Unit'}")
        print(sep)
        par_rows = [
            ("Psi_ZFW=Ixx_ZFW/|Cl_p|", Psi_ZFW_d,  se_PZW_d,  "slug.ft2"),
            ("slope=sigma/|Cl_p|",       slope_d,    se_slope_d,"slug.ft2/slug_fuel"),
            ("eta=Cl_da/|Cl_p|",         eta_d,      theta_se_d[2], ""),
            ("mu_r=Cl_r/|Cl_p|",         theta_d[3], theta_se_d[3], ""),
            ("mu_β=Cl_β/|Cl_p|",         theta_d[4], theta_se_d[4], ""),
            ("mu_dr=Cl_dr/|Cl_p|",       theta_d[5], theta_se_d[5], ""),
        ]
        for name, val, se, unit in par_rows:
            print(f"  {name:<22}  {val:>12.5g}  {2*se:>12.4g}  {unit}")
        print(sep)
        print(f"\nDerived:")
        print(f"  |Cl_p|   = {Cl_p_abs_d:>10.5f}  /rad")
        print(f"  Cl_p     = {-Cl_p_abs_d:>10.5f}  /rad")
        print(f"  Cl_da    = {Cl_da_d:>10.5f}  /rad")
        print(f"\n  Ixx_ZFW  = {Ixx_ZFW_d:>10.1f}  +/-2se={2*se_Ixx_d:.1f}  slug.ft2")
        print(f"  n_total  = {n_d:,}    resid_std = {resid_d:.4g} lb·ft")

        # Ixx at mean fleet fuel
        m_fuel_arr = np.array([sd["fuel_mean_lb"] for sd in accepted])
        mean_fuel_lb   = float(np.mean(m_fuel_arr))
        mean_fuel_slug = mean_fuel_lb / G_FPS2
        Ixx_at_mean_d  = Ixx_ZFW_d + sigma * mean_fuel_slug
        print(f"  Ixx at mean fuel ({mean_fuel_lb:.0f} lb) = {Ixx_at_mean_d:.1f} slug.ft2")

        out_path = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
        output_d = {
            "method":          "direct",
            "sigma_ft2":       sigma,
            "y_tank_ft":       y_tank,
            "Ixx_ZFW":         Ixx_ZFW_d,
            "se_Ixx_ZFW":      se_Ixx_d,
            "Cl_p_raw":        -Cl_p_abs_d,
            "se_Cl_p":         float(sigma / slope_d**2 * se_slope_d),
            "Cl_da":           Cl_da_d,
            "resid_std":       resid_d,
            "n_total":         n_d,
            "n_sorties":       len(accepted),
            "mean_fuel_lb":    mean_fuel_lb,
            "Ixx_at_mean_fuel": Ixx_at_mean_d,
            "rejected":        rejected,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output_d, f, indent=2)
        print(f"\nResults saved to {out_path}")
        return

    print(f"\n{'='*55}")
    print(f"Stage 1: hierarchical regression  ({len(accepted)} sorties)")
    print(f"{'='*55}")

    theta, theta_se, resid_std, n_per = stage1_hierarchical(accepted, verbose=args.verbose)
    if theta is None:
        print("Stage 1 failed — singular system.")
        sys.exit(1)

    N         = len(accepted)
    Psi_vals  = theta[:N]
    glob      = theta[N:N+N_GLOB]      # [eta, mu_r, mu_beta, mu_dr]
    eta       = glob[0]
    Psi_se    = theta_se[:N]          if theta_se is not None else np.full(N, np.nan)
    glob_se   = theta_se[N:N+N_GLOB]  if theta_se is not None else np.full(N_GLOB, np.nan)
    eta_se    = glob_se[0]

    glob_names = ["eta(Cl_da/|Cp|)", "mu_r(Cl_r/|Cp|)", "mu_b(Clb/|Cp|)", "mu_dr(Cldr/|Cp|)"]
    for gn, gv, gs in zip(glob_names, glob, glob_se):
        print(f"  {gn:22s} = {gv:+.5g}  +/-2se={2*gs:.3g}")
    print(f"residual std        = {resid_std:.4g} ft.lb")
    print()
    print(f"Per-sortie Psi = Ixx_total/|Cl_p|:")
    m_fuel_arr = np.array([sd["fuel_mean_lb"] for sd in accepted])  # lb
    m_slug_arr = m_fuel_arr / G_FPS2

    for i, sd in enumerate(accepted):
        ok = n_per[i] >= 50 if i < len(n_per) else False
        psi_str = f"{Psi_vals[i]:.1f} +/-{Psi_se[i]:.2f}" if ok else "skip"
        print(f"  {sd['sortie']:25s}  Psi={psi_str:>18s}  fuel={sd['fuel_mean_lb']:.0f} lb")

    print(f"\n{'='*55}")
    print(f"Stage 2: Psi vs fuel-mass regression")
    print(f"{'='*55}")

    result, err = stage2_fuel_regression(
        Psi_vals, Psi_se, m_slug_arr, sigma, verbose=args.verbose
    )
    if result is None:
        print(f"Stage 2 failed: {err}")
        sys.exit(1)

    Cl_p_abs = result["Cl_p_abs"]
    Ixx_ZFW  = result["Ixx_ZFW"]
    Cl_da    = eta * Cl_p_abs    # signed
    Cl_p     = -Cl_p_abs

    sep = "-" * 55
    print(f"\n{sep}")
    print(f"{'Parameter':<18}  {'Estimate':>12}  {'+/-2se':>10}  {'Unit'}")
    print(sep)
    print(f"{'Ixx_ZFW':<18}  {Ixx_ZFW:>12.1f}  {2*result['se_Ixx_ZFW']:>10.1f}  slug.ft2")
    print(f"{'Cl_p':<18}  {Cl_p:>12.5f}  {2*result['se_Cl_p']:>10.5f}  1/rad")
    print(f"{'Cl_da':<18}  {Cl_da:>12.5f}  {'---':>10}  1/rad")
    print(f"{'eta=Cl_da/|Cl_p|':<18}  {eta:>12.5f}  {2*eta_se:>10.5f}  dimensionless")
    print(f"{'sigma (y_tank^2)':<18}  {sigma:>12.1f}  {'---':>10}  ft^2")
    print(f"{'y_tank':<18}  {y_tank:>12.3f}  {'---':>10}  ft")
    print(sep)
    print(f"Stage 2  R^2 = {result['r2_stage2']:.4f}  ({result['n_sorties']} sorties)")
    slope_slugs = result["slope"]   # Psi per slug of fuel
    print(f"Slope sigma/|Cl_p| = {slope_slugs:.4f}  (expected ~{sigma/Cl_p_abs:.4f})")

    mean_fuel_lb = float(np.mean(m_fuel_arr))
    mean_fuel_slug = mean_fuel_lb / G_FPS2
    Ixx_at_mean = Ixx_ZFW + sigma * mean_fuel_slug
    print(f"Ixx_total at mean fuel ({mean_fuel_lb:.0f} lb) = {Ixx_at_mean:.1f} slug.ft2")

    # ── Save ─────────────────────────────────────────────────────────────────
    out_sorties = []
    for i, sd in enumerate(accepted):
        out_sorties.append({
            "sortie":        sd["sortie"],
            "n_samples":     sd["n_samples"],
            "fuel_mean_lb":  sd["fuel_mean_lb"],
            "pdot_rms_dps":  sd["pdot_rms_dps"],
            "Psi":           float(Psi_vals[i]),
            "Psi_se":        float(Psi_se[i]),
            "Ixx_total_est": float(Psi_vals[i] * Cl_p_abs),
        })

    output = {
        "sigma_ft2":        sigma,
        "y_tank_ft":        y_tank,
        "eta":              float(eta),
        "eta_se":           float(eta_se),
        "Ixx_ZFW":          Ixx_ZFW,
        "se_Ixx_ZFW":       result["se_Ixx_ZFW"],
        "Cl_p":             Cl_p,
        "se_Cl_p":          result["se_Cl_p"],
        "Cl_da":            Cl_da,
        "stage2_r2":        result["r2_stage2"],
        "stage1_resid_std": resid_std,
        "n_sorties":        len(accepted),
        "sorties":          out_sorties,
        "rejected":         rejected,
    }
    out_path = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")
    print(f"  {len(rejected)} sorties rejected")


if __name__ == "__main__":
    main()
