"""
sync_trace_graph_map.py — update batch_config.json trace_graph_map from _info.json files.

_info.json fcc_version is authoritative. For each sortie that has a version,
derive the base key (strip subsegment number), group subsegments, and update
the map. Subsegments that disagree get individual keys.
"""

import json
import glob
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "batch_config.json")

# Matches subsegment number before the tail suffix, e.g. S116_2_N208B -> S116_N208B
_SUBSEG_RE = re.compile(r'^(.+?)_(\d+)_(N208B|ZKMLN)$')


def base_key(sortie_label):
    """S116_2_N208B -> S116_N208B,  S116_N208B -> S116_N208B"""
    m = _SUBSEG_RE.match(sortie_label)
    if m:
        return f"{m.group(1)}_{m.group(3)}"
    return sortie_label


def main():
    # Load config (may have cp1252-encoded chars in comments)
    with open(CONFIG_PATH, encoding="cp1252") as f:
        cfg = json.load(f)
    old_map = cfg.get("trace_graph_map", {})

    # Collect fcc_version from all _info.json files
    info_files = sorted(glob.glob(
        os.path.join(ROOT, "data", "**", "*_info.json"), recursive=True
    ))

    # base_key -> {version: [sortie_labels]}
    by_base = {}
    for path in info_files:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        ver = d.get("fcc_version")
        if not ver:
            continue
        label = d.get("sortie", "")
        bk = base_key(label)
        by_base.setdefault(bk, {}).setdefault(ver, []).append(label)

    # Build new map entries from _info.json
    new_entries = {}  # key -> version
    for bk, versions in sorted(by_base.items()):
        if len(versions) == 1:
            # All subsegments agree
            ver = next(iter(versions))
            new_entries[bk] = ver
        else:
            # Subsegments disagree — use individual keys
            for ver, labels in versions.items():
                for label in labels:
                    new_entries[label] = ver

    # Compute diff vs old map
    corrected = {}
    added = {}
    for key, ver in new_entries.items():
        if key in old_map:
            if old_map[key] != ver:
                corrected[key] = (old_map[key], ver)
        else:
            # Check if a base key in old_map covers this (prefix match)
            covered = any(
                key.startswith(ok.replace("_N208B", "").replace("_ZKMLN", ""))
                for ok in old_map
            )
            added[key] = ver

    # Build merged map: start from old, apply new_entries (info.json wins)
    merged = dict(old_map)
    merged.update(new_entries)

    # Sort: G before S, then numerically within each group
    def sort_key(k):
        m = re.match(r'^([A-Z])(\d+)', k)
        if m:
            return (m.group(1), int(m.group(2)), k)
        return ('Z', 0, k)

    merged_sorted = dict(sorted(merged.items(), key=lambda kv: sort_key(kv[0])))

    # Report
    if corrected:
        print(f"Corrected ({len(corrected)}) — _info.json overrides trace_graph_map:")
        for key, (old, new) in sorted(corrected.items()):
            print(f"  {key:<30s}  {old}  ->  {new}")
    else:
        print("No corrections needed (existing entries all match _info.json)")

    print(f"\nAdded {len(added)} new entries from _info.json:")
    for key, ver in sorted(added.items(), key=lambda kv: sort_key(kv[0])):
        print(f"  {key:<30s}  {ver}")

    print(f"\ntrace_graph_map: {len(old_map)} -> {len(merged_sorted)} entries")

    # Write back
    cfg["trace_graph_map"] = merged_sorted
    with open(CONFIG_PATH, "w", encoding="cp1252") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")
    print(f"Updated {CONFIG_PATH}")


if __name__ == "__main__":
    main()
