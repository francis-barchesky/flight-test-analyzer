#!/usr/bin/env python3
"""
patch_all_sorties.py — Batch patch a missing signal into every sortie that
lacks it, by downloading the raw IADS ZIP from S3 and running a local export.

Requires the iads-export repo with its ie_venv activated (for IADS COM access).

Usage:
    python patch_all_sorties.py \
        --iads-export-dir C:/path/to/iads-export \
        --data-group C:/path/to/iads-export/config/patch_sysNotEngage.csv \
        [--signal sysNotEngage] \
        [--workers 2] \
        [--dry-run]
"""

import argparse
import glob
import json
import multiprocessing
import os
import re
import sys

import boto3

RAW_BUCKET = 'merlin-pilot-iads-data-raw'
SEARCH_DIR = 'N208B'

# Root of this repo (flight-test-analyzer)
_FTA_DIR = os.path.dirname(os.path.abspath(__file__))


def _local_to_s3_name(local_dir: str) -> str:
    """S094_2_N208B → S094N208B_2,  S101_N208B → S101N208B"""
    m = re.match(r'^(S\d+)_(\d+)_(N208B)$', local_dir)
    if m:
        return f'{m.group(1)}{m.group(3)}_{m.group(2)}'
    m = re.match(r'^(S\d+)_(N208B)$', local_dir)
    if m:
        return f'{m.group(1)}{m.group(2)}'
    return local_dir


def _find_raw_zip(s3_client, sortie_s3: str):
    prefix = f'{SEARCH_DIR}/{sortie_s3}/'
    resp = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=prefix)
    zips = [o['Key'] for o in resp.get('Contents', []) if o['Key'].endswith('.zip')]
    if not zips:
        return None
    return f's3://{RAW_BUCKET}/{zips[0]}'


def _has_signal(sortie_path: str, signal: str) -> bool:
    jsons = [j for j in glob.glob(f'{sortie_path}/analysis_*.json')
             if '_hires' not in j]
    if not jsons:
        return False
    try:
        with open(jsons[0]) as f:
            return signal in json.load(f)
    except Exception:
        return False


def _worker(args):
    """Runs in a separate process — each process gets its own COM state."""
    local_dir, sortie_s3, raw_uri, data_group, out_dir, signal, iads_export_dir = args

    import sys, os
    sys.path.insert(0, iads_export_dir)  # patch_export_local + iads_export package
    sys.path.insert(0, _FTA_DIR)         # patch_signal_from_zip

    from patch_export_local import run as export_run
    import patch_signal_from_zip as psz

    try:
        print(f'[{sortie_s3}] exporting ...')
        ok = export_run(raw_uri, data_group, out_dir)
        if not ok:
            return local_dir, 'export failed'

        dg_stem = os.path.splitext(os.path.basename(data_group))[0]
        zip_path = os.path.join(out_dir, f'{dg_stem}_{sortie_s3}.zip')
        if not os.path.isfile(zip_path):
            return local_dir, 'zip not found after export'

        print(f'[{sortie_s3}] patching ...')
        patched = psz.patch(zip_path, signal, data_root=out_dir)
        if not patched:
            return local_dir, 'patch failed or already present'

        try:
            os.remove(zip_path)
        except OSError:
            pass

        return local_dir, 'ok'

    except Exception as e:
        return local_dir, f'exception: {e}'


def main():
    p = argparse.ArgumentParser(
        description='Batch patch a missing signal into all sortie analysis JSONs.')
    p.add_argument('--iads-export-dir', required=True,
                   help='Path to iads-export repo root (must have ie_venv activated)')
    p.add_argument('--data-group', required=True,
                   help='Local CSV of signal paths (e.g. iads-export/config/patch_sysNotEngage.csv)')
    p.add_argument('--signal', default='sysNotEngage',
                   help='Signal name to patch (default: sysNotEngage)')
    p.add_argument('--workers', type=int, default=2,
                   help='Parallel worker processes (default: 2)')
    p.add_argument('--dry-run', action='store_true',
                   help='Print what would run without doing it')
    p.add_argument('--force', action='store_true',
                   help='Re-export and re-patch even if signal already present')
    args = p.parse_args()

    fta            = _FTA_DIR
    iads_export    = os.path.abspath(args.iads_export_dir)
    dg             = os.path.abspath(args.data_group)

    if not os.path.isdir(iads_export):
        sys.exit(f'ERROR: iads-export-dir not found: {iads_export}')
    if not os.path.isfile(dg):
        sys.exit(f'ERROR: data-group not found: {dg}')

    # Collect sortie directories in this repo
    sortie_dirs = sorted(
        d for d in os.listdir(fta)
        if re.match(r'^S\d+.*N208B$', d) and os.path.isdir(os.path.join(fta, d))
    )

    if not args.force:
        sortie_dirs = [d for d in sortie_dirs
                       if not _has_signal(os.path.join(fta, d), args.signal)]

    if not sortie_dirs:
        print('All sorties already have the signal. Use --force to re-patch.')
        return

    print(f'Signal  : {args.signal}')
    print(f'Sorties : {len(sortie_dirs)} to process')
    print(f'Workers : {args.workers}')
    print()

    # Resolve S3 URIs
    s3 = boto3.client('s3')
    work_items = []
    skipped = []
    for local_dir in sortie_dirs:
        s3_name = _local_to_s3_name(local_dir)
        raw_uri = _find_raw_zip(s3, s3_name)
        if not raw_uri:
            skipped.append((local_dir, 'no raw ZIP in S3'))
            continue
        out_dir = os.path.join(fta, local_dir)
        work_items.append((local_dir, s3_name, raw_uri, dg, out_dir, args.signal, iads_export))

    if skipped:
        print(f'Skipping {len(skipped)} (no raw ZIP in S3):')
        for d, reason in skipped:
            print(f'  {d}: {reason}')
        print()

    if not work_items:
        print('Nothing to process.')
        return

    if args.dry_run:
        print('Dry run — would process:')
        for local_dir, s3_name, raw_uri, *_ in work_items:
            print(f'  {local_dir:25s}  ←  {raw_uri}')
        return

    results = {'ok': [], 'failed': []}
    with multiprocessing.Pool(processes=args.workers) as pool:
        for local_dir, status in pool.imap_unordered(_worker, work_items):
            if status == 'ok':
                results['ok'].append(local_dir)
                print(f'  ✓  {local_dir}')
            else:
                results['failed'].append((local_dir, status))
                print(f'  ✗  {local_dir}: {status}')

    print()
    print(f'Done — {len(results["ok"])} patched, {len(results["failed"])} failed.')
    if results['failed']:
        print('Failed:')
        for d, r in results['failed']:
            print(f'  {d}: {r}')


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
