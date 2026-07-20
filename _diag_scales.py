import json, numpy as np, math
from scipy.signal import savgol_filter

path = r"S143_1_N208B\analysis_S143_1_hires.json"
with open(path) as f:
    d = json.load(f)
fp = d["flight_plots"]

def sig(name):
    raw = np.array(fp[name], dtype=float)
    return raw[:,0], raw[:,1]

t_p, p    = sig("bodyRollRateVotedValue")
_,   daL  = sig("Left_Aileron_lvdt2deg")
_,   daR  = sig("Right_Aileron_lvdt2deg")
_,   tas  = sig("tasVoted")
_,   fuel = sig("lt_fuel_wt")
_,   wt   = sig("WT_current")

da = 0.5*(daL - daR)

print(f"p   (deg/s):  min={p.min():.2f}  max={p.max():.2f}  std={p.std():.2f}")
print(f"da  (deg):    min={da.min():.2f}  max={da.max():.2f}  std={da.std():.2f}")
print(f"TAS (kts):    min={tas.min():.1f}  max={tas.max():.1f}  mean={tas.mean():.1f}")
print(f"fuel_L (lb):  min={fuel.min():.1f}  max={fuel.max():.1f}  range={fuel.max()-fuel.min():.1f}")
print(f"WT_current:   min={wt.min():.1f}  max={wt.max():.1f}")

dt = np.median(np.diff(t_p))
print(f"\nbody-rate dt: {dt:.3f}s  ({1/dt:.2f} Hz)")

p_rad  = p * math.pi/180
win    = max(5, int(round(5.0/dt)) | 1)
p_dot  = savgol_filter(p_rad, win, 3, deriv=1, delta=dt)
print(f"p_dot (rad/s2): max|.|={np.abs(p_dot).max():.5f}  std={p_dot.std():.5f}")

V_fps  = tas.mean() * 1.68781
qbar   = 0.5 * 0.002377 * V_fps**2
qSb    = qbar * 279.0 * 52.1
print(f"\nq_bar = {qbar:.1f} lb/ft2   q_bar*S*b = {qSb:.0f} ft.lb")
da_rad_std = da.std() * math.pi/180
print(f"Typical L_aero ~ qSb*Cl_da*da_std = {qSb:.0f} * 0.08 * {da_rad_std:.5f} = {qSb*0.08*da_rad_std:.1f} ft.lb")
print(f"Ixx_assumed*p_dot_std ~ 5000 * {p_dot.std():.5f} = {5000*p_dot.std():.2f} ft.lb")
print(f"\nz = -(sigma*m_fuel)*p_dot ~ -{49*(fuel.mean()/32.174):.1f}*p_dot_std = {-49*(fuel.mean()/32.174)*p_dot.std():.2f} ft.lb")
print(f"\nMismatch ratio (L_aero / Ixx*p_dot): {(qSb*0.08*da_rad_std)/(5000*p_dot.std()):.1f}x")
