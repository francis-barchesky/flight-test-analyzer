"""Skip 150,000 rows (15 min at 100 Hz) then sample 500 cruise rows from S007N208B_2."""
import zipfile
import struct
import inflate64

SIGNALS = [
    "Time",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyRollRate",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyPitchRate",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyYawRate",
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcTas",
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcPressAlt",
    "Left_Aileron_lvdt2deg",
    "Right_Aileron_lvdt2deg",
    "Rudder_lvdt2deg",
    "Elevator_lvdt2deg",
    "RDC1_RDC1_A429_TX_1_247_Left_Fuel_Quantity",
    "RDC1_RDC1_A429_TX_1_255_Right_Fuel_Quantity",
    "WT_current",
    "CG_current",
    "AoS1_rvdt2deg",
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyLatAccel",
]

zpath = r'C:\Users\FrancisBarchesky\Documents\GitHub\flight-test-analyzer\S007N208B_IadsDataExport.zip'

SKIP_ROWS  = 150_000   # skip ~15 min
SAMPLE_ROWS = 500


def get_data_offset(fz, entry):
    fz.seek(entry.header_offset)
    fz.read(4)
    fz.read(22)
    fn_len = struct.unpack('<H', fz.read(2))[0]
    ex_len = struct.unpack('<H', fz.read(2))[0]
    fz.read(fn_len + ex_len)
    return fz.tell()


def stream_lines(fz, data_offset):
    fz.seek(data_offset)
    inflater = inflate64.Inflater()
    leftover = b''
    while not inflater.eof:
        chunk = fz.read(131072)
        if not chunk:
            break
        data = inflater.inflate(chunk)
        if not data:
            continue
        block = leftover + data
        lines = block.split(b'\n')
        leftover = lines[-1]
        for line in lines[:-1]:
            yield line.decode('utf-8', errors='replace').rstrip('\r')
    if leftover:
        yield leftover.decode('utf-8', errors='replace').rstrip('\r')


with open(zpath, 'rb') as fz:
    z = zipfile.ZipFile(fz)
    entry = next(e for e in z.infolist() if '_2_' in e.filename)
    offset = get_data_offset(fz, entry)
    gen = stream_lines(fz, offset)

    header_line = next(gen)
    header = [h.strip() for h in header_line.split(',')]

    col_idx = {}
    for sig in SIGNALS:
        try:
            col_idx[sig] = header.index(sig)
        except ValueError:
            print(f"  MISSING: {sig}")

    print(f"Skipping {SKIP_ROWS:,} rows (~{SKIP_ROWS/100/60:.0f} min)...", flush=True)
    for i, _ in enumerate(gen):
        if i + 1 >= SKIP_ROWS:
            break

    print(f"Sampling {SAMPLE_ROWS} rows...", flush=True)
    buckets = {s: [] for s in col_idx}
    n_rows = 0
    first_time = None
    for line in gen:
        row = line.split(',')
        if first_time is None:
            try:
                first_time = row[col_idx.get("Time", 0)]
            except Exception:
                pass
        for sig, j in col_idx.items():
            try:
                buckets[sig].append(float(row[j]))
            except (ValueError, IndexError):
                pass
        n_rows += 1
        if n_rows >= SAMPLE_ROWS:
            break

print(f"\nRows sampled: {n_rows}  (starting around row {SKIP_ROWS:,})")
if first_time:
    print(f"First time stamp in sample: {first_time}")
print(f"\n{'Signal':<60} {'min':>10} {'max':>10} {'mean':>10}")
print('-'*95)
for sig in SIGNALS:
    if sig not in buckets or not buckets[sig]:
        print(f"  {sig:<58} (no data)")
        continue
    vals = buckets[sig]
    print(f"  {sig:<58} {min(vals):>10.3f} {max(vals):>10.3f} {sum(vals)/len(vals):>10.3f}")
