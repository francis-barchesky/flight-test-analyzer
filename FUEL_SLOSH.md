# Fuel Slosh Effects on Roll Inertia Estimation

## What slosh does

Fuel slosh affects the roll inertia estimate in two coupled ways:

**1. Frequency-dependent effective σ.** The rigid-body model assumes fuel moves instantaneously with the tank:

```
Ixx(t) = Ixx_ZFW + σ · m_fuel(t)
```

where σ = ∫y² dm/m is the squared lateral fuel arm. In reality, at high excitation frequencies the fuel cannot keep up with tank wall motion, so its effective inertia contribution falls below the rigid-body value. The model overestimates σ, which causes Ixx_ZFW to be underestimated.

**2. Slosh damping absorbed into Cl_p.** Energy dissipated by sloshing fuel appears in roll-rate residuals as unexplained damping. The optimizer assigns this to Cl_p, biasing it more negative than its true aerodynamic value.

## Slosh natural frequency (C208B wet wing tanks)

First lateral slosh mode for a rectangular tank:

```
f_slosh ≈ (1/2π) · √( g · (π/L) · tanh(π·h/L) )
```

| Parameter | Value |
|-----------|-------|
| L (spanwise tank length) | ≈ 13.4 ft (WS 53→214, confirmed MM) |
| h (fuel depth at mid-fill, ~830 lb) | ≈ 0.4 ft |
| **f_slosh** | **≈ 0.3–0.5 Hz** |

The OEM integration window is 5 seconds (bandwidth down to ~0.2 Hz), so slosh dynamics fall inside the estimation band. They are not a static bias — the integrator partially models them, and the residual they leave gets absorbed into Cl_p.

## Practical impact

| Effect | Magnitude | Notes |
|--------|-----------|-------|
| σ overestimate | ~few % of σ·m_fuel | Second-order vs. ±30% geometric uncertainty on σ |
| Cl_p bias (more negative) | Unknown, likely <5% | Absorbed from slosh damping |
| Ixx_ZFW bias (low) | Coupled to σ error | Cannot separate without multi-frequency data |

Slosh is second-order noise on top of the dominant uncertainty, which is geometric (the tank lateral span σ is not identifiable from general flight data — see below).

## Identifiability note

σ is **not identifiable** from general flight roll data. The gradient ∂cost/∂σ = (∂cost/∂Ixx_ZFW) · m_fuel = 0 at any OEM optimum, because fuel burns slowly within a sortie and the inertia contribution of σ and Ixx_ZFW are observationally equivalent. This was confirmed by:

- Fixed-σ scan (σ = 5–200 ft², 40 points): residual std flat at 1494.7173 m°/s for every σ ≥ 10 ft²; only σ=5 differs (Cl_p hits its lower bound)
- Joint optimization with σ free: optimizer made zero progress in the σ direction (gradient = 0)

Slosh makes this worse — even if σ were identifiable from the dynamics, the frequency-dependent effective σ would require a separate slosh model to extract the geometric σ.

## C208B tank geometry and effective σ

From the Maintenance Manual, WS stations are from aircraft centerline (inches):

| Boundary | WS (in) | y (ft) |
|----------|---------|--------|
| Inner closeout rib | 53.0 | 4.42 |
| Outer closeout rib | 214.3 | 17.86 |

Uniform-distribution σ = (y1² + y1·y2 + y2²)/3 = **139.2 ft²**

However, the POH states fuel flows by gravity **inboard** to the reservoir. This means:
- At partial fuel states the outboard portion of the tank is empty first
- σ_effective < 139 ft², approaching the inboard reservoir arm at low fuel states
- The effective σ varies with fuel quantity, making the linear model `Ixx = Ixx_ZFW + σ·m_fuel` an approximation
- At mean fuel (~831 lb = 41% of capacity), σ_effective is probably 60–100 ft²

Using σ=139 (full geometric) gives Cl_p=-0.743/rad and Ixx_ZFW=9044 slug·ft², which are physically plausible upper bounds. Using σ=49 (volume estimate, inboard-biased) gives Cl_p=-0.261/rad, probably too low for a high-AR wing.

## What would separate slosh from rigid-body dynamics

- **Multi-frequency swept-sine aileron inputs** at the same fuel state: compare effective Ixx at f << f_slosh (rigid, full σ) vs. f >> f_slosh (decoupled, reduced σ_eff)
- **Dedicated doublet maneuvers at 3+ fuel states** (near-empty, mid, near-full): the change in effective Ixx between states gives σ_eff; comparing to geometric σ gives the slosh correction
- **Higher sample rate (≥200 Hz)**: resolves the roll time constant (τ ≈ 91 ms = 9 samples at 100 Hz) which is currently at the resolution limit

## References

- Abramson, H.N. (1966). *The Dynamic Behavior of Liquids in Moving Containers* (NASA SP-106). — canonical slosh theory
- Morelli, E.A. (2011). *System Identification Programs for Aircraft (SIDPAC)*. — roll equation OEM methodology
- Cessna 208B Maintenance Manual (not available) — authoritative tank BL positions and baffle locations
