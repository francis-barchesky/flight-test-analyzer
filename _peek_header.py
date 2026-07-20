import zipfile
import struct
import inflate64
import sys

zpath = sys.argv[1]

def get_data_offset(fz, entry):
    fz.seek(entry.header_offset)
    fz.read(4)
    fz.read(22)
    fn_len = struct.unpack('<H', fz.read(2))[0]
    ex_len = struct.unpack('<H', fz.read(2))[0]
    fz.read(fn_len + ex_len)
    return fz.tell()

with open(zpath, 'rb') as fz:
    z = zipfile.ZipFile(fz)
    for entry in z.infolist():
        print(f"\n=== {entry.filename}  ({entry.file_size:,} bytes, compress_type={entry.compress_type}) ===")
        if entry.compress_type == 9:
            offset = get_data_offset(fz, entry)
            fz.seek(offset)
            inflater = inflate64.Inflater()
            chunk = fz.read(131072)
            data = inflater.inflate(chunk)
            line = data.split(b'\n')[0].decode('utf-8', errors='replace').strip()
            cols = line.split(',')
            print(f"Columns ({len(cols)}): {', '.join(cols[:20])} ...")
            # check for key signals
            key_sigs = ['Left_Aileron_lvdt2deg', 'votedIrsBodyRollRate', 'votedAdcTas',
                        'WT_current', 'UDPCM_maneuver_type']
            for sig in key_sigs:
                present = any(sig in c for c in cols)
                print(f"  {sig}: {'YES' if present else 'no'}")
        else:
            print(f"  (compress_type={entry.compress_type}, skipping)")
