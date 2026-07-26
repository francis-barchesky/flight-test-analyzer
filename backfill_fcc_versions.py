"""
backfill_fcc_versions.py — populate fcc_version in _info.json files from trace_graph_map.

For each _info.json with fcc_version=null, looks up the sortie's base key in
trace_graph_map (stripping subsegment numbers) and writes the version if found.
Does not overwrite existing non-null values.
"""

import json
import glob
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "batch_config.json")

_SUBSEG_RE = re.compile(r'^(.+?)_(\d+)_(N208B|ZKMLN)$')


def base_key(sortie_label):
    m = _SUBSEG_RE.match(sortie_label)
    if m:
        return f"{m.group(1)}_{m.group(3)}"
    return sortie_label


def main():
    with open(CONFIG_PATH, encoding="cp1252") as f:
        cfg = json.load(f)
    tgm = cfg.get("trace_graph_map", {})

    info_files = sorted(glob.glob(
        os.path.join(ROOT, "data", "**", "*_info.json"), recursive=True
    ))

    updated = 0
    skipped_no_match = []

    for path in info_files:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)

        if d.get("fcc_version"):
            continue  # already set — don't overwrite

        label = d.get("sortie", "")
        bk = base_key(label)
        ver = tgm.get(bk)

        if not ver:
            skipped_no_match.append(label)
            continue

        d["fcc_version"] = ver
        d["version_source"] = "trace_graph_map"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
            f.write("\n")

        print(f"  {label:<35s}  {ver}")
        updated += 1

    print(f"\nUpdated {updated} _info.json files.")
    print(f"No match in trace_graph_map: {len(skipped_no_match)} sorties")
    if skipped_no_match:
        for s in skipped_no_match:
            print(f"  {s}")


if __name__ == "__main__":
    main()
