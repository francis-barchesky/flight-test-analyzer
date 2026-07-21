#!/usr/bin/env python3
"""
Generate N chunk batch configs for parallel pipeline execution.

Each chunk gets:
  - A non-overlapping slice of the base config's date range
  - Its own data_root (pipeline_chunks/data_N) so instances don't collide
  - workers and skip_existing overridden as specified

Prints the POSIX paths of the generated config files to stdout (one per line)
so run_parallel_pipeline.sh can consume them.

Usage:
  python setup_pipeline_chunks.py [batch_config.json] [--chunks 8] [--workers 3]
"""
import argparse
import datetime
import json
import pathlib
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base_config", nargs="?", default="batch_config.json")
    ap.add_argument("--chunks",  type=int, default=8)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--out-dir", default="pipeline_chunks")
    args = ap.parse_args()

    base_path = pathlib.Path(args.base_config).resolve()
    out_dir   = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(base_path) as f:
        base = json.load(f)

    start_str = base.get("download_start_date", datetime.date.today().isoformat())
    end_str   = base.get("download_end_date",   datetime.date.today().isoformat())
    if isinstance(end_str, str) and end_str.lower() == "today":
        end_str = datetime.date.today().isoformat()

    start = datetime.date.fromisoformat(start_str)
    end   = datetime.date.fromisoformat(end_str)
    n     = args.chunks

    total_days = (end - start).days + 1
    chunk_days = (total_days + n - 1) // n  # ceiling division

    chunks = []
    d = start
    for _ in range(n):
        cs = d
        ce = min(d + datetime.timedelta(days=chunk_days - 1), end)
        chunks.append((cs, ce))
        d = ce + datetime.timedelta(days=1)
        if d > end:
            break

    config_paths = []
    for i, (cs, ce) in enumerate(chunks, 1):
        data_subdir = f"pipeline_chunks/data_{i}"
        (pathlib.Path(data_subdir)).mkdir(parents=True, exist_ok=True)

        cfg = dict(base)
        cfg["download_start_date"] = cs.isoformat()
        cfg["download_end_date"]   = ce.isoformat()
        cfg["data_root"]           = data_subdir  # relative to project root; both pipeline.sh and run_batch.py resolve from __file__
        cfg["workers"]             = args.workers
        cfg["skip_existing"]       = False

        cfg_path = out_dir / f"batch_config_{i}.json"
        with open(cfg_path, "w") as f:
            json.dump(cfg, f, indent=2)

        # Emit POSIX path for bash to consume
        p = str(cfg_path.resolve())
        if len(p) >= 2 and p[1] == ":":  # Windows absolute path -> Git Bash POSIX
            p = "/" + p[0].lower() + p[2:].replace("\\", "/")
        config_paths.append(p)

        print(f"  Chunk {i:2d}: {cs} -> {ce}  ({data_subdir})", file=sys.stderr)

    print("\n".join(config_paths))


if __name__ == "__main__":
    main()
