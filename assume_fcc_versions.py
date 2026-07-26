"""
assume_fcc_versions.py — fill fcc_version gaps using chronological interpolation.

For each tail (N208B, ZKMLN), sorties are ordered by rec_start_s from their
analysis JSON. For any sortie missing fcc_version, if its nearest confirmed
neighbour on each side agree on the same version, that version is assigned with
version_source="assumed". If neighbours disagree (version changed in the gap),
the sortie is left null — we can't safely infer.

Run after make_sortie_info.py and sync_trace_graph_map.py.
"""

import gzip
import glob
import json
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT_MAP = {
    'N208B': os.path.join(ROOT, 'data', 'N208B'),
    'ZKMLN': os.path.join(ROOT, 'data', 'ZKMLN'),
}


def _rec_start(sortie_dir):
    """Return rec_start_s from the analysis JSON, or None if unavailable."""
    patterns = [
        os.path.join(sortie_dir, 'analysis_*.json.gz'),
        os.path.join(sortie_dir, 'rs_analysis_*.json.gz'),
        os.path.join(sortie_dir, 'analysis_*.json'),
    ]
    for pat in patterns:
        hits = glob.glob(pat)
        if not hits:
            continue
        path = max(hits, key=os.path.getmtime)
        try:
            opener = gzip.open if path.endswith('.gz') else open
            with opener(path, 'rt', encoding='utf-8') as f:
                # Only read first 512 bytes to find rec_start_s quickly
                head = f.read(512)
            m = re.search(r'"rec_start_s"\s*:\s*([\d.]+)', head)
            if m:
                return float(m.group(1))
        except Exception:
            pass
    return None


def _info_path(sortie_dir, sortie_name):
    return os.path.join(sortie_dir, f'{sortie_name}_info.json')


def main():
    total_assumed = 0

    for tail, data_root in DATA_ROOT_MAP.items():
        if not os.path.isdir(data_root):
            continue

        # Collect all sorties with analysis JSONs
        sorties = []
        for entry in sorted(os.scandir(data_root), key=lambda e: e.name):
            if not entry.is_dir():
                continue
            t = _rec_start(entry.path)
            if t is None:
                continue
            info_p = _info_path(entry.path, entry.name)
            info = {}
            if os.path.exists(info_p):
                try:
                    with open(info_p, encoding='utf-8') as f:
                        info = json.load(f)
                except Exception:
                    pass
            sorties.append({
                'name':      entry.name,
                'dir':       entry.path,
                'info_path': info_p,
                'rec_start': t,
                'fcc':       info.get('fcc_version'),
                'source':    info.get('version_source'),
                'info':      info,
            })

        sorties.sort(key=lambda s: s['rec_start'])

        # Extract confirmed version at each index (None if unset/assumed)
        confirmed = [s['fcc'] if s['source'] != 'assumed' else None for s in sorties]

        assumed_this_tail = 0
        for i, s in enumerate(sorties):
            if s['fcc'] and s['source'] != 'assumed':
                continue  # already confirmed

            # Find nearest confirmed neighbour on each side
            prev_ver = next((confirmed[j] for j in range(i - 1, -1, -1) if confirmed[j]), None)
            next_ver = next((confirmed[j] for j in range(i + 1, len(sorties)) if confirmed[j]), None)

            if not prev_ver and not next_ver:
                continue  # no anchors at all
            if prev_ver and next_ver and prev_ver != next_ver:
                continue  # version changed across this gap — can't infer

            assumed_ver = prev_ver or next_ver

            # Write back
            info = dict(s['info'])
            info['sortie'] = s['name']
            info['fcc_version'] = assumed_ver
            info['version_source'] = 'assumed'
            with open(s['info_path'], 'w', encoding='utf-8') as f:
                json.dump(info, f, indent=2)
                f.write('\n')

            print(f"  {s['name']:<35s}  {assumed_ver}  (assumed from {prev_ver or '?'} / {next_ver or '?'})")
            assumed_this_tail += 1

        print(f"\n{tail}: assumed {assumed_this_tail} version(s)")
        total_assumed += assumed_this_tail

    print(f"\nTotal assumed: {total_assumed}")
    print("Run sync_trace_graph_map.py to push assumed versions into batch_config.json.")


if __name__ == '__main__':
    main()
