"""
extract_old_csv.py — extract AFCS signals from old deflate64 IADS CSV exports
(S007N208B format: single ZIP with two large CSVs, compress_type=9/deflate64).

Writes .afcs_signals.npz compatible with estimate_inertia.py (SCHEMA_VERSION=2).

Usage:
    python extract_old_csv.py S007N208B_IadsDataExport.zip
    python extract_old_csv.py S007N208B_IadsDataExport.zip --out S007N208B_2_N208B/
"""
import argparse
import hashlib
import io
import os
import struct
import sys
import zipfile

import inflate64
import numpy as np
from scipy.interpolate import interp1d

SCHEMA_VERSION = 2
CACHE_FILE = ".afcs_signals.npz"

# lbs/gal for Jet-A (used to convert fuel quantity in gallons → lbs)
JET_A_LB_PER_GAL = 6.7
KTS2FPS = 1.68781

# Old-format CSV column → (logical_name, scale_factor)
# scale_factor converts from CSV units to the units expected by estimate_inertia.py:
#   p/q/r  : deg/s   (no conversion needed)
#   tas     : ft/s    (multiply knots by KTS2FPS)
#   press_alt: ft     (no conversion)
#   daL/daR/dr/de: deg  (no conversion)
#   mL/mR   : lbs    (multiply gallons by JET_A_LB_PER_GAL)
#   wt      : lbs    (no conversion)
#   cg      : in     (no conversion)
#   ay      : ft/s²  (no conversion)
OLD_SIGNAL_MAP = {
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyRollRate":  ("p",         1.0),
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyPitchRate": ("q",         1.0),
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyYawRate":   ("r",         1.0),
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcTas":           ("tas",       KTS2FPS),
    "FCC1A_g_voting_mdlrefdw_rtb_votedAdcPressAlt":      ("press_alt", 1.0),
    "Left_Aileron_lvdt2deg":                             ("daL",       1.0),
    "Right_Aileron_lvdt2deg":                            ("daR",       1.0),
    "Rudder_lvdt2deg":                                   ("dr",        1.0),
    "Elevator_lvdt2deg":                                 ("de",        1.0),
    "RDC1_RDC1_A429_TX_1_247_Left_Fuel_Quantity":        ("mL",        JET_A_LB_PER_GAL),
    "RDC1_RDC1_A429_TX_1_255_Right_Fuel_Quantity":       ("mR",        JET_A_LB_PER_GAL),
    "WT_current":                                        ("wt",        1.0),
    "CG_current":                                        ("cg",        1.0),
    "FCC1A_g_voting_mdlrefdw_rtb_votedIrsBodyLatAccel":  ("ay",        1.0),
}


def _parse_time(s):
    """Parse DDD:HH:MM:SS.sss or HH:MM:SS.sss or plain float → seconds."""
    parts = s.split(":")
    if len(parts) == 4:
        d, h, m, sec = parts
        return int(d) * 86400 + int(h) * 3600 + int(m) * 60 + float(sec)
    if len(parts) == 3:
        h, m, sec = parts
        return int(h) * 3600 + int(m) * 60 + float(sec)
    return float(s)


def _get_data_offset(fz, entry):
    fz.seek(entry.header_offset)
    fz.read(4)    # local file signature
    fz.read(22)   # fixed fields
    fn_len = struct.unpack('<H', fz.read(2))[0]
    ex_len = struct.unpack('<H', fz.read(2))[0]
    fz.read(fn_len + ex_len)
    return fz.tell()


def _stream_lines(fz, data_offset):
    """Yield decoded text lines from a deflate64 compressed stream."""
    fz.seek(data_offset)
    inflater = inflate64.Inflater()
    leftover = b''
    while not inflater.eof:
        chunk = fz.read(262144)  # 256 KB compressed chunks
        if not chunk:
            break
        decompressed = inflater.inflate(chunk)
        if not decompressed:
            continue
        block = leftover + decompressed
        lines = block.split(b'\n')
        leftover = lines[-1]
        for line in lines[:-1]:
            yield line.decode('utf-8', errors='replace').rstrip('\r')
    if leftover:
        yield leftover.decode('utf-8', errors='replace').rstrip('\r')


def extract_entry(fz, entry, verbose=False):
    """
    Stream-extract one CSV entry and return {logical_name: (t_array, y_array)}.
    """
    offset = _get_data_offset(fz, entry)
    gen = _stream_lines(fz, offset)

    # Parse header
    header_line = next(gen)
    header = [h.strip() for h in header_line.split(',')]

    time_col = None
    for i, h in enumerate(header):
        if h.lower().startswith("time"):
            time_col = i
            break
    if time_col is None:
        print(f"  ERROR: no Time column in {entry.filename}", file=sys.stderr)
        return {}

    # Map wanted CSV columns to (logical_name, scale, col_index)
    wanted = {}   # csv_col → (logical, scale, idx)
    for csv_col, (logical, scale) in OLD_SIGNAL_MAP.items():
        try:
            idx = header.index(csv_col)
            wanted[csv_col] = (logical, scale, idx)
        except ValueError:
            if verbose:
                print(f"  MISSING column: {csv_col}", file=sys.stderr)

    if not wanted:
        return {}

    buckets = {info[0]: [] for info in wanted.values()}  # logical → [(t, y)]
    n_rows = 0
    for line in gen:
        if not line:
            continue
        row = line.split(',')
        if len(row) <= time_col:
            continue
        try:
            t = _parse_time(row[time_col])
        except Exception:
            continue
        for csv_col, (logical, scale, idx) in wanted.items():
            if idx >= len(row):
                continue
            v = row[idx]
            if not v or v.lower() == 'nan':
                continue
            try:
                buckets[logical].append((t, float(v) * scale))
            except ValueError:
                pass
        n_rows += 1
        if n_rows % 100_000 == 0:
            print(f"  ... {n_rows:,} rows", flush=True)

    print(f"  Parsed {n_rows:,} rows from {entry.filename}")

    # Convert to numpy arrays, sort, deduplicate
    result = {}
    for logical, pts in buckets.items():
        if len(pts) < 2:
            continue
        arr = np.asarray(pts, dtype=float)
        order = np.argsort(arr[:, 0], kind='stable')
        arr = arr[order]
        keep = np.concatenate(([True], np.diff(arr[:, 0]) > 0))
        result[logical] = (arr[keep, 0], arr[keep, 1])
    return result


def _save_npz(out_path, key, found):
    names = list(found.keys())
    # also save total_fuel = mL + mR for convenience
    if 'mL' in found and 'mR' in found:
        tL, yL = found['mL']
        tR, yR = found['mR']
        # interpolate mR onto mL time grid
        t_common = tL
        f_mR = interp1d(tR, yR, bounds_error=False, fill_value=np.nan)
        yR_i = f_mR(t_common)
        found['fuel'] = (t_common, yL + yR_i)
        names.append('fuel')

    payload = {
        '_key':   np.array(key, dtype='<U64'),
        '_names': np.array('|'.join(names), dtype='<U4096'),
    }
    for name, (t, y) in found.items():
        payload[f'{name}__t'] = t
        payload[f'{name}__y'] = y
    np.savez_compressed(out_path, **payload)
    print(f"  Saved {len(names)} signals to {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('zip_path', help='Path to the deflate64 ZIP file')
    ap.add_argument('--out', default=None,
                    help='Output directory (default: basename from CSV name)')
    ap.add_argument('--entry', default=None,
                    help='Substring to select one CSV entry (default: all)')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    zip_path = os.path.abspath(args.zip_path)
    if not os.path.exists(zip_path):
        sys.exit(f"Not found: {zip_path}")

    with open(zip_path, 'rb') as fz:
        z = zipfile.ZipFile(fz)
        entries = [e for e in z.infolist() if e.filename.lower().endswith('.csv')]
        if args.entry:
            entries = [e for e in entries if args.entry in e.filename]
        print(f"Found {len(entries)} CSV entries:")
        for e in entries:
            print(f"  {e.filename}  ({e.file_size:,} bytes, compress_type={e.compress_type})")
        print()

        for entry in entries:
            # Determine output directory from CSV filename
            # e.g. "S007N208B_2_IadsDataExport.csv" → "S007_2_N208B"
            stem = entry.filename.replace('_IadsDataExport.csv', '')
            # Try to make a directory name
            if args.out:
                out_dir = args.out
            else:
                # stem is e.g. "S007N208B" or "S007N208B_2"
                # transform to sortie dir convention
                if '_' in stem:
                    parts = stem.split('_')
                    sortie_parts = '_'.join(parts[:-1]) if len(parts) > 1 else stem
                    suffix = parts[-1]
                    # Rearrange to match "S007_2_N208B" convention
                    # stem like "S007N208B_2" → "S007_2_N208B"
                    base = sortie_parts.replace('N208B', '')
                    out_dir = os.path.join(os.path.dirname(zip_path),
                                          f"{base}_{suffix}_N208B")
                else:
                    out_dir = os.path.join(os.path.dirname(zip_path),
                                          f"{stem.replace('N208B', '')}_N208B")

            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, CACHE_FILE)

            st = os.stat(zip_path)
            key_str = f"schema:v{SCHEMA_VERSION}|{os.path.basename(zip_path)}:{st.st_mtime_ns}:{st.st_size}|{entry.filename}"
            key = hashlib.sha256(key_str.encode()).hexdigest()

            print(f"Extracting {entry.filename} -> {out_dir}/")
            found = extract_entry(fz, entry, verbose=args.verbose)
            if not found:
                print(f"  No signals found, skipping.")
                continue
            _save_npz(out_path, key, found)
            print()


if __name__ == '__main__':
    main()
