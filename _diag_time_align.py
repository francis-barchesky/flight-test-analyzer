"""Check time-base alignment between .raw_signals_cache.npz and .afcs_signals.npz."""
import glob, os
import numpy as np
from scipy.interpolate import interp1d

ROOT = os.path.dirname(os.path.abspath(__file__))

SORTIES_CHECK = None   # None = all

dirs_all = sorted(
    d for d in glob.glob(os.path.join(ROOT, "*_N208B"))
    if os.path.isdir(d) and not os.path.basename(d).startswith("G")
    and os.path.exists(os.path.join(d, ".raw_signals_cache.npz"))
    and os.path.exists(os.path.join(d, ".afcs_signals.npz"))
)

print(f"{'Sortie':<25}  {'xcorr_r':>8}  {'lag_ms':>7}  {'overlap_h':>9}")
print("-"*55)

for d in dirs_all:
    name = os.path.basename(d)
    if not os.path.isdir(d):
        continue
    npz_path  = os.path.join(d, ".raw_signals_cache.npz")
    afcs_path = os.path.join(d, ".afcs_signals.npz")
    if not os.path.exists(npz_path) or not os.path.exists(afcs_path):
        print(f"{name}: missing cache")
        continue

    npz  = np.load(npz_path,  allow_pickle=False)
    afcs = np.load(afcs_path, allow_pickle=False)

    t_p   = npz["roll_rate__t"]
    t_da  = afcs["daL__t"]
    y_da  = afcs["daL__y"]
    y_p   = npz["roll_rate__y"]

    # Time range comparison
    print(f"\n{'='*60}")
    print(f"{name}")
    print(f"  NPZ  roll_rate : t=[{t_p[0]:.1f}, {t_p[-1]:.1f}]  n={len(t_p):,}  dt={np.median(np.diff(t_p))*1000:.1f}ms")
    print(f"  AFCS daL      : t=[{t_da[0]:.1f}, {t_da[-1]:.1f}]  n={len(t_da):,}  dt={np.median(np.diff(t_da))*1000:.1f}ms")

    # Overlap
    t_start = max(t_p[0],  t_da[0])
    t_end   = min(t_p[-1], t_da[-1])
    overlap_s = t_end - t_start
    print(f"  Overlap       : {overlap_s:.1f} s  ({overlap_s/3600:.2f} hr)")

    if overlap_s < 10:
        print("  *** NO MEANINGFUL OVERLAP — time bases may be mismatched ***")
        continue

    # Cross-correlate da and p on common grid to check lag
    dt    = float(np.median(np.diff(t_p)))
    t_uni = np.arange(t_start, t_end, dt)
    if len(t_uni) < 1000:
        continue
    p_g  = interp1d(t_p,  y_p,  bounds_error=False, fill_value=np.nan)(t_uni)
    da_g = interp1d(t_da, y_da, bounds_error=False, fill_value=np.nan)(t_uni)

    ok = np.isfinite(p_g) & np.isfinite(da_g)
    p_ok  = p_g[ok]  - p_g[ok].mean()
    da_ok = da_g[ok] - da_g[ok].mean()

    # Normalised cross-correlation over ±2 s window (200 lags at 100 Hz)
    max_lag = 200
    n = len(p_ok)
    if n < 2 * max_lag + 100:
        print("  Too short for xcorr")
        continue

    norm = np.std(p_ok) * np.std(da_ok) * n
    lags = range(-max_lag, max_lag + 1)
    xcorr = [float(np.dot(p_ok[max_lag:n-max_lag],
                           da_ok[max_lag+lag:n-max_lag+lag]) / norm)
             for lag in lags]
    best_lag = list(lags)[int(np.argmax(np.abs(xcorr)))]
    best_xcorr = xcorr[best_lag + max_lag]
    print(f"  xcorr(p, da)  : peak r={best_xcorr:.3f} at lag={best_lag} samples ({best_lag*dt*1000:.0f} ms)")
    print(f"  (positive lag means da leads p, as expected for causal aerodynamics)")
