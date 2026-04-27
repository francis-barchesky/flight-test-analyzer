#!/usr/bin/env python3
"""
patch_sysnotengage.py — Back-fill sysNotEngage transitions into existing analysis JSONs.

Scans each analysis_*.json for sysNotEngage transitions already present in:
  - result["transitions"]          (flat / no-trigger mode)
  - result["episodes"][*]["transitions"]  (trigger mode)

Deduplicates, sorts by time, and writes result["sysNotEngage"] back in-place.

NOTE: transitions that fell outside every episode window are not recoverable
      without a full re-run of analyze_iads.py.

Usage:
    python patch_sysnotengage.py                    # scans current directory
    python patch_sysnotengage.py /path/to/data_root
    python patch_sysnotengage.py --force            # overwrite even if key exists
"""

import json
import os
import re
import sys

_SNE_RE = re.compile(r'sysnotengage', re.I)


def _is_sne(signal):
    return bool(_SNE_RE.search(signal.split(".")[-1]))


def patch_file(json_path, force=False):
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    if data.get("sysNotEngage") and not force:
        return "skip", "already has sysNotEngage"

    seen = set()
    collected = []

    def _add(t):
        key = t.get("signal", "") + "|" + str(t.get("time", ""))
        if key not in seen:
            seen.add(key)
            collected.append(t)

    for t in data.get("transitions", []):
        if _is_sne(t.get("signal", "")):
            _add(t)

    for ep in data.get("episodes", []):
        for t in ep.get("transitions", []):
            if _is_sne(t.get("signal", "")):
                _add(t)

    if not collected:
        return "skip", "sysNotEngage not found in this JSON"

    try:
        collected.sort(key=lambda t: float(t["time"]))
    except (ValueError, TypeError):
        pass

    data["sysNotEngage"] = collected
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), allow_nan=False)

    rises = sum(1 for t in collected if t.get("to") == 1)
    return "patched", f"{len(collected)} transition(s), {rises} assertion(s)"


def main():
    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    data_root = args[0] if args else "."

    targets = []
    for entry in sorted(os.listdir(data_root)):
        full = os.path.join(data_root, entry)
        if os.path.isfile(full) and entry.startswith("analysis_") and entry.endswith(".json") and "_hires" not in entry:
            targets.append(full)
        elif os.path.isdir(full) and not entry.startswith("."):
            for f in sorted(os.listdir(full)):
                if f.startswith("analysis_") and f.endswith(".json") and "_hires" not in f:
                    targets.append(os.path.join(full, f))

    if not targets:
        print("No analysis_*.json files found.")
        return

    patched = skipped = 0
    for path in targets:
        status, msg = patch_file(path, force=force)
        name = os.path.relpath(path, data_root)
        if status == "patched":
            print(f"  patched  {name}  ({msg})")
            patched += 1
        else:
            print(f"  skipped  {name}  ({msg})")
            skipped += 1

    print(f"\nDone — {patched} patched, {skipped} skipped.")


if __name__ == "__main__":
    main()
