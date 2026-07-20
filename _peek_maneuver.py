"""Peek at individual test-point CSVs in a ZIP — show header and time-series stats."""
import zipfile
import csv
import io
import sys

SIGNALS_OF_INTEREST = [
    "Time",
    # body rates
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyRollRate",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyPitchRate",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyYawRate",
    # surfaces
    "Left_Aileron_lvdt2deg",
    "Right_Aileron_lvdt2deg",
    "Rudder_lvdt2deg",
    "Elevator_lvdt2deg",
    # TAS / alt
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcTas",
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcPressAlt",
    # fuel / weight
    "RDC1_RDC1_A429_TX_1_247_Left_Fuel_Quantity",
    "RDC1_RDC1_A429_TX_1_255_Right_Fuel_Quantity",
    "WT_current",
    # UDPCM
    "UDPCM_maneuver_type",
    "UDPCM_test_point_id",
    "UDPCM_is_active",
]

zpath = sys.argv[1]
entry_filter = sys.argv[2] if len(sys.argv) > 2 else None

with zipfile.ZipFile(zpath) as z:
    entries = [e for e in z.infolist() if e.filename.lower().endswith('.csv')]
    if entry_filter:
        entries = [e for e in entries if entry_filter in e.filename]

    for entry in entries[:5]:  # first 5 matching
        print(f"\n{'='*70}")
        print(f"  {entry.filename}  ({entry.file_size/1e6:.1f} MB)")
        print(f"{'='*70}")
        with z.open(entry) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding='utf-8', errors='replace'))
            header = [h.strip() for h in next(reader)]
            col_idx = {}
            for sig in SIGNALS_OF_INTEREST:
                try:
                    col_idx[sig] = header.index(sig)
                except ValueError:
                    pass

            present = [s for s in SIGNALS_OF_INTEREST if s in col_idx]
            missing = [s for s in SIGNALS_OF_INTEREST if s not in col_idx]
            if missing:
                print(f"  Missing: {', '.join(s.split('_')[-1] for s in missing)}")

            buckets = {s: [] for s in present}
            n = 0
            for row in reader:
                for sig in present:
                    j = col_idx[sig]
                    if j < len(row) and row[j] and row[j].lower() != 'nan':
                        try:
                            buckets[sig].append(float(row[j]))
                        except ValueError:
                            pass
                n += 1

        print(f"  Rows: {n}")
        print(f"\n  {'Signal':<40} {'min':>10} {'max':>10} {'rms':>10}")
        print(f"  {'-'*74}")
        for sig in present:
            if sig == "Time":
                continue
            vals = buckets[sig]
            if not vals:
                continue
            mn = min(vals)
            mx = max(vals)
            rms = (sum(v*v for v in vals)/len(vals))**0.5
            label = sig.split('_')[-1] if len(sig) > 30 else sig
            print(f"  {label:<40} {mn:>10.3f} {mx:>10.3f} {rms:>10.3f}")
