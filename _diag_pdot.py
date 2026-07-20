"""Quick diagnostic: per-sortie roll maneuver content from .raw_signals_cache.npz."""
import glob, math, os
import numpy as np
from scipy.interpolate import interp1d
from scipy.signal import savgol_filter

ROOT    = os.path.dirname(os.path.abspath(__file__))
DEG2RAD = math.pi / 180
KTS2FPS = 1.68781

dirs = sorted(
    d for d in glob.glob(os.path.join(ROOT, "*_N208B"))
    if os.path.isdir(d) and not os.path.basename(d).startswith("G")
    and os.path.exists(os.path.join(d, ".raw_signals_cache.npz"))
)

print(f"{'Sortie':<25}  {'n_OK':>7}  {'max|pdot|°/s²':>13}  {'rms°/s²':>8}  {'fr>1°/s²':>8}  {'max|p|°/s':>10}")
print("-" * 80)

for d in dirs:
    npz = np.load(os.path.join(d, ".raw_signals_cache.npz"), allow_pickle=False)
    if "roll_rate__t" not in npz:
        continue
    t     = npz["roll_rate__t"]
    p_deg = npz["roll_rate__y"]
    p_rad = p_deg * DEG2RAD
    dt    = float(np.median(np.diff(t)))
    if dt <= 0 or dt > 0.05:
        continue
    win   = max(5, int(round(0.3 / dt)) | 1)
    p_dot = savgol_filter(p_rad, win, 3, deriv=1, delta=dt)   # rad/s²

    # airborne mask
    mask = np.ones(len(t), dtype=bool)
    if "tas__t" in npz and "tas__y" in npz:
        tas = interp1d(npz["tas__t"], npz["tas__y"],
                       bounds_error=False, fill_value=np.nan)(t)
        mask &= (tas > 50 * KTS2FPS)
    mask &= np.isfinite(p_dot)

    if mask.sum() < 100:
        continue

    p_ok  = p_dot[mask]
    pd_deg = np.degrees(p_ok)
    n_ok  = mask.sum()
    max_pd = float(np.max(np.abs(pd_deg)))
    rms_pd = float(np.sqrt(np.mean(pd_deg ** 2)))
    fr_hi  = float(np.mean(np.abs(pd_deg) > 1.0))     # fraction > 1 °/s²
    max_p  = float(np.nanmax(np.abs(p_deg[mask])))

    name = os.path.basename(d)
    print(f"  {name:<25}  {n_ok:>7,}  {max_pd:>13.2f}  {rms_pd:>8.4f}  {fr_hi:>8.3f}  {max_p:>10.2f}")
