#!/usr/bin/env python3
"""
make_sortie_info.py — generate/update sortie_info.json

For every sortie that has an analysis JSON in the pipeline data roots, look up:
  - Jira issue key + URL (FFT project, issuetype Flight Test)
  - FCC SW version (Google Drive flight card first, then Jira Post-Flight Report)
  - FTS SW version (Google Drive flight card, if present)

Output: sortie_info.json  (in the same directory as this script)

Usage:
    python make_sortie_info.py              # incremental — skip sorties already complete
    python make_sortie_info.py --all        # re-query every sortie
    python make_sortie_info.py --dry-run    # discover sorties, print names, no network calls

Credentials are read from ~/.cia_config.json (same file used by CIA generator and run_batch.py).
Required fields:
    jiraToken, jiraAuthType, jiraCloudId   — for Jira lookups
    googleToken                            — for Google Drive lookups (needs drive.readonly scope)

If the Google token is stale (401/403), re-run setupCiaAuth.m (MATLAB) to refresh it.
"""

import argparse
import gzip
import json
import os
import pathlib
import re
import sys
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).parent
CONFIG_PATH = HERE / 'batch_config.json'
OUT_PATH = HERE / 'sortie_info.json'


# ── Version normalization ─────────────────────────────────────────────────────

def _pad_ver(v):
    """Normalize to X.XX.XX (major unpadded, minor+patch 2-digit zero-padded)."""
    if not v:
        return None
    parts = v.split('.')
    if len(parts) != 3:
        return v
    try:
        major, minor, patch = (int(p) for p in parts)
        return f'{major:02d}.{minor:02d}.{patch:02d}'
    except ValueError:
        return v


# ── Sortie tag normalization ──────────────────────────────────────────────────

def _search_tag(sortie_name):
    """Return the bare sortie+tail tag used for Jira/GDrive searches.

    Strips leg numbers AND timestamps from the middle segment so that
    S140_1_N208B, S007_20260623165549_N208B, and S140_N208B all resolve
    to the same search tag (S140N208B / S007N208B).
    """
    parts = sortie_name.split('_')
    if len(parts) == 3 and parts[1].isdigit():
        return parts[0] + parts[2]   # drop numeric middle (leg or timestamp)
    return sortie_name.replace('_', '')


# ── Credentials ───────────────────────────────────────────────────────────────

def _load_creds():
    cfg_path = pathlib.Path.home() / '.cia_config.json'
    if not cfg_path.exists():
        return {}
    try:
        return json.loads(cfg_path.read_text(encoding='utf-8-sig'))
    except Exception:
        return {}


# ── Sortie discovery ──────────────────────────────────────────────────────────

def _find_analysis_json(sortie_dir):
    """Return path to the analysis JSON(.gz) in a sortie directory, or None."""
    d = pathlib.Path(sortie_dir)
    for pat in ('rs_analysis_*.json.gz', 'rs_analysis_*.json',
                'analysis_*.json.gz', 'analysis_*.json'):
        matches = sorted(d.glob(pat))
        if matches:
            return str(matches[-1])
    return None


def discover_sorties(data_root_map, base_dir):
    """
    Walk every tail's data root and return a list of dicts:
        {sortie, tail, sortie_dir, analysis_json}
    sorted by sortie name.
    """
    entries = []
    for tail, rel_root in data_root_map.items():
        data_root = (base_dir / rel_root).resolve()
        if not data_root.is_dir():
            print(f"  [warn] data root not found: {data_root}", flush=True)
            continue
        for child in sorted(data_root.iterdir()):
            if not child.is_dir():
                continue
            json_path = _find_analysis_json(child)
            if json_path:
                entries.append({
                    'sortie': child.name,
                    'tail': tail,
                    'sortie_dir': child,
                    'analysis_json': json_path,
                })
    entries.sort(key=lambda e: e['sortie'])
    return entries


# ── Google Drive lookup ───────────────────────────────────────────────────────

def _gdrive_lookup(sortie_name, token):
    """
    Search Google Drive for a flight-card doc whose title contains the sortie
    tag (e.g. 'S140N208B'), export as plain text, and extract all SW versions.

    Returns (fcc_version, fts_version, pdi_version, morgana_version) — any may be None.

    Supported formats:
      N208B: "FCC - 05.01.04" / "FTS - 05.01.02" / "PDI - 5.04.00" / "Morgana - c41fadc"
      ZKMLN: "FCC LOAD: 5.00.07"
    """
    if not token:
        return None, None, None, None, None

    search_tag = _search_tag(sortie_name)
    query = (f"name contains '{search_tag}' and "
             f"mimeType = 'application/vnd.google-apps.document'")
    search_url = (
        'https://www.googleapis.com/drive/v3/files'
        f'?q={urllib.parse.quote(query)}'
        '&fields=files(id,name)'
        '&includeItemsFromAllDrives=true'
        '&supportsAllDrives=true'
        '&pageSize=3'
    )
    try:
        req = urllib.request.Request(search_url, headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception:
        return None, None, None, None

    files = data.get('files', [])
    if not files:
        return None, None, None, None

    for f in files:
        try:
            export_url = (
                f'https://www.googleapis.com/drive/v3/files/{f["id"]}/export'
                '?mimeType=text/plain'
            )
            result = _parse_versions_from_gdoc(export_url, token)
            if any(result):
                return result
        except Exception:
            continue

    return None, None, None, None


def _parse_versions_from_gdoc(export_url, token):
    """Fetch a GDoc export URL and extract FCC/FTS/PDI/Morgana versions."""
    try:
        req = urllib.request.Request(export_url, headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode('utf-8', errors='replace')
    except Exception:
        return None, None, None, None

    # Version pattern: 1–2 digit groups, e.g. "5.1.4" or "05.01.04"
    _ver = r'(\d{1,2}\.\d{1,2}\.\d{1,2})'

    fcc = None
    m = re.search(r'\bFCC\b[^\n]{0,25}?' + _ver, content)
    if m:
        fcc = m.group(1)

    fts = None
    m = re.search(r'\bFTS\b[^\n]{0,25}?' + _ver, content)
    if m:
        fts = m.group(1)

    pdi = None
    m = re.search(r'\bPDI\b[^\n]{0,25}?' + _ver, content)
    if m:
        pdi = m.group(1)

    morgana = None
    m = re.search(r'\bMorgana\b[^\n]{0,25}?([0-9a-fA-F]{7,40})\b', content)
    if m:
        morgana = m.group(1).lower()

    return fcc, fts, pdi, morgana


def _gdrive_fetch_by_id(file_id, token):
    """Fetch a GDoc by Drive file ID and extract versions."""
    export_url = (
        f'https://www.googleapis.com/drive/v3/files/{file_id}/export'
        '?mimeType=text/plain'
    )
    return _parse_versions_from_gdoc(export_url, token)


# ── Jira lookup ───────────────────────────────────────────────────────────────

def _jira_lookup(sortie_name, creds, cache=None):
    """
    Search FFT Jira for a Flight Test issue matching the sortie name.
    Returns (jira_key, jira_url, fcc_version, test_card_url) — any may be None.

    Multi-leg sorties (S140_1_N208B, S140_2_N208B, …) all resolve to the same
    base tag (S140N208B) and therefore the same Jira issue. Pass a dict as
    `cache` to avoid redundant API calls across legs of the same sortie.

    Checks:
      1. Issue summary for a version string (ground-test issues)
      2. Post Flight Report (customfield_10042) for "FCC load X.XX.XX"
    """
    token = creds.get('jiraToken', '')
    auth_type = creds.get('jiraAuthType', 'Basic')
    if not token:
        return None, None, None, None

    search_tag = _search_tag(sortie_name)
    if cache is not None and search_tag in cache:
        return cache[search_tag]

    jql = (f'project = FFT AND issuetype = "Flight Test" '
           f'AND summary ~ "{search_tag}" ORDER BY updated DESC')
    url = ('https://merlinlabs.atlassian.net/rest/api/3/search/jql'
           f'?jql={urllib.parse.quote(jql)}'
           f'&fields=key,summary,customfield_10041,customfield_10042&maxResults=1')

    try:
        req = urllib.request.Request(url, headers={
            'Authorization': f'{auth_type} {token}',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception:
        return None, None, None, None

    issues = data.get('issues', [])
    if not issues:
        return None, None, None, None

    issue = issues[0]
    key = issue.get('key')
    jira_url = f'https://merlinlabs.atlassian.net/browse/{key}' if key else None
    fields = issue.get('fields', {})

    # Test Card URL (customfield_10041) — Google Doc link from the Jira issue
    test_card_url = None
    tc = fields.get('customfield_10041')
    if tc:
        # Field is a rich-text doc; extract the first URL
        tc_text = json.dumps(tc)
        m = re.search(r'https://docs\.google\.com/\S+', tc_text)
        if m:
            test_card_url = m.group(0).rstrip('",\\')

    # 1. Version in summary — require X.XX.XX format to avoid matching dates (e.g. 5.20.2026)
    summary = fields.get('summary', '')
    m = re.search(r'\b(\d{1,2}\.\d{2}\.\d{2})\b', summary)
    fcc_ver = m.group(1) if m else None

    # 2. Post Flight Report (customfield_10042)
    if not fcc_ver:
        pfr = fields.get('customfield_10042')
        if pfr:
            pfr_text = json.dumps(pfr)
            m = re.search(r'\bFCC\b[^"]{0,40}?(\d{1,2}\.\d{1,2}\.\d{1,2})\b', pfr_text)
            if m:
                fcc_ver = m.group(1)

    result = key, jira_url, fcc_ver, test_card_url
    if cache is not None:
        cache[search_tag] = result
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--all', action='store_true',
                    help='Re-query every sortie (ignore existing entries)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Discover sorties and print them; no network calls')
    ap.add_argument('--config', default=str(CONFIG_PATH),
                    help='Path to batch_config.json (default: %(default)s)')
    ap.add_argument('--out', default=str(OUT_PATH),
                    help='Output JSON path (default: %(default)s)')
    args = ap.parse_args()

    cfg_path = pathlib.Path(args.config)
    if not cfg_path.exists():
        print(f'ERROR: config not found: {cfg_path}', file=sys.stderr)
        sys.exit(1)
    cfg = json.loads(cfg_path.read_text())

    data_root_map = cfg.get('data_root_map', {})
    if not data_root_map:
        # Fall back to single data_root
        data_root = cfg.get('data_root', 'data/N208B')
        tail = 'N208B' if 'N208B' in data_root else 'UNKNOWN'
        data_root_map = {tail: data_root}

    base_dir = cfg_path.parent

    print('Discovering sorties...', flush=True)
    sorties = discover_sorties(data_root_map, base_dir)
    print(f'  Found {len(sorties)} sorties with analysis JSONs', flush=True)

    if args.dry_run:
        for e in sorties:
            print(f"  {e['sortie']}  ({e['tail']})  →  {e['sortie_dir']}")
        return

    creds = _load_creds()
    google_token = creds.get('googleToken', '')
    jira_cache = {}  # search_tag -> (key, url, fcc_ver); shared across legs of same sortie

    consolidated = {}
    updated = 0
    skipped = 0

    for i, e in enumerate(sorties, 1):
        name = e['sortie']
        sortie_dir = e['sortie_dir']
        per_sortie_path = sortie_dir / f'{name}_info.json'

        # Incremental: skip if per-sortie file already has a jira_key, unless --all
        if not args.all and per_sortie_path.exists():
            try:
                existing = json.loads(per_sortie_path.read_text())
                if existing.get('jira_key'):
                    consolidated[name] = existing
                    skipped += 1
                    # Seed the cache so sibling legs can reuse this result
                    parts = name.split('_')
                    tag = (parts[0] + parts[2]) if len(parts) == 3 and parts[1].isdigit() else name.replace('_', '')
                    jira_cache.setdefault(tag, (existing.get('jira_key'), existing.get('jira_url'), existing.get('fcc_version'), existing.get('test_card_url')))
                    continue
            except Exception:
                pass

        print(f'[{i}/{len(sorties)}]  {name}', end='', flush=True)

        # Google Drive — primary source
        fcc_ver, fts_ver, pdi_ver, morgana_ver = _gdrive_lookup(name, google_token)
        src = 'gdrive' if fcc_ver else ''

        # Jira — key/URL always; FCC version as fallback; cache shared across legs
        jira_key, jira_url, jira_fcc, test_card_url = _jira_lookup(name, creds, cache=jira_cache)

        if not fcc_ver and jira_fcc:
            fcc_ver = jira_fcc
            src = 'jira'

        # Fallback: fetch test card directly by URL if GDrive name search missed it
        if not fcc_ver and test_card_url and google_token:
            m = re.search(r'/d/([A-Za-z0-9_-]+)', test_card_url)
            if m:
                tc_fcc, tc_fts, tc_pdi, tc_morgana = _gdrive_fetch_by_id(m.group(1), google_token)
                if tc_fcc:
                    fcc_ver = tc_fcc
                    src = 'gdrive'
                if not fts_ver and tc_fts:
                    fts_ver = tc_fts
                if not pdi_ver and tc_pdi:
                    pdi_ver = tc_pdi
                if not morgana_ver and tc_morgana:
                    morgana_ver = tc_morgana

        entry = {
            'sortie': name,
            'jira_key': jira_key,
            'jira_url': jira_url,
            'test_card_url': test_card_url,
            'fcc_version': _pad_ver(fcc_ver),
            'fts_version': _pad_ver(fts_ver),
            'pdi_version': _pad_ver(pdi_ver),
            'morgana_version': morgana_ver,
            'version_source': src or None,
        }
        per_sortie_path.write_text(json.dumps(entry, indent=2))
        consolidated[name] = entry
        updated += 1

        parts = []
        if jira_key:
            parts.append(jira_key)
        if fcc_ver:
            parts.append(f'FCC={fcc_ver}')
        if fts_ver:
            parts.append(f'FTS={fts_ver}')
        if pdi_ver:
            parts.append(f'PDI={pdi_ver}')
        if morgana_ver:
            parts.append(f'Morgana={morgana_ver}')
        print(f'  ->  {", ".join(parts) if parts else "(no data)"}', flush=True)

    # Write consolidated sortie_info.json alongside the script
    out_path = pathlib.Path(args.out)
    out_path.write_text(json.dumps(dict(sorted(consolidated.items())), indent=2))
    print(f'\nDone. {updated} updated, {skipped} skipped, {len(consolidated)} total.', flush=True)
    print(f'Per-sortie _info.json written to each sortie directory.', flush=True)
    print(f'Consolidated: {out_path}', flush=True)


if __name__ == '__main__':
    main()
