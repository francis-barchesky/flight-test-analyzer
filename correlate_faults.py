#!/usr/bin/env python3
"""
correlate_faults.py — Scan all analysis_*.json files, extract transitions in
the pre-trigger window around each fault episode, and rank signals by how
consistently they change in that window across the fleet.

Writes fault_correlations.json to the data root.

Usage:
    python correlate_faults.py [data_root]
    python correlate_faults.py . --window-pre 5 --window-post 1 --min-freq 0.05
"""

import argparse
import fnmatch
import glob
import json
import math
import os
import re
import sys
from collections import defaultdict


# ---------------------------------------------------------------------------
# Signal normalisation
# ---------------------------------------------------------------------------

_LANE_RE   = re.compile(r'^(FCC1A|FCC1B|RDC1|RDC2|IRS1|IRS2)\.')
_MODULE_RE = re.compile(r'^g_(.+?)(?:_mdlrefdw|_mdlref|_mdl)?\.')


def _sfx(signal: str) -> str:
    """Return the bare signal suffix: FCC1A.g_foo_mdlrefdw.rtb.barBaz → barBaz"""
    return signal.split('.')[-1]


def _lane(signal: str) -> str:
    m = _LANE_RE.match(signal)
    return m.group(1) if m else ''


def _model(signal: str) -> str:
    parts = signal.split('.')
    if len(parts) < 2:
        return ''
    m = _MODULE_RE.match(parts[1])
    if not m:
        return ''
    raw = m.group(1)
    for suffix in ('_mdlrefdw', '_mdlref', '_mdl'):
        raw = raw.replace(suffix, '')
    return raw


def _norm_key(signal: str) -> str:
    """Deduplication key: model.suffix (lane-agnostic)."""
    s = _sfx(signal)
    mo = _model(signal)
    return f'{mo}.{s}' if mo else s


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load_sortie(path: str):
    """Return list of (trigger_time, transitions_in_episode) for one JSON."""
    try:
        with open(path, encoding='utf-8') as f:
            d = json.load(f)
    except Exception:
        return []
    episodes = d.get('episodes', [])
    out = []
    for ep in episodes:
        try:
            t_trig = float(ep['start_time'])
        except (KeyError, ValueError, TypeError):
            continue
        out.append((t_trig, ep.get('transitions', []), os.path.basename(path)))
    return out


# ---------------------------------------------------------------------------
# Core correlation
# ---------------------------------------------------------------------------

def _correlate(all_episodes, window_pre: float, window_post: float):
    """
    all_episodes: list of (trigger_time, transitions, sortie_name)

    Returns list of dicts, sorted by score descending.
    """
    n_total = len(all_episodes)
    if n_total == 0:
        return []

    # Per signal-key: collect occurrences
    # { norm_key: { 'dts': [], 'sorties': set(), 'episodes': set(),
    #               'transitions': [(from,to)] } }
    signal_data = defaultdict(lambda: {
        'dts': [], 'sorties': set(), 'ep_ids': set(), 'trans': []
    })

    for ep_idx, (t_trig, transitions, sortie) in enumerate(all_episodes):
        seen_this_ep = set()
        for tr in transitions:
            try:
                t = float(tr['time'])
            except (ValueError, TypeError):
                continue
            dt = t - t_trig
            if dt < -window_pre or dt > window_post:
                continue

            key = _norm_key(tr['signal'])
            rec = signal_data[key]
            rec['dts'].append(dt)
            rec['sorties'].add(sortie)
            rec['ep_ids'].add(ep_idx)
            if len(rec['trans']) < 200:
                rec['trans'].append((tr.get('from'), tr.get('to')))
            seen_this_ep.add(key)

    results = []
    for key, rec in signal_data.items():
        ep_count   = len(rec['ep_ids'])
        frequency  = ep_count / n_total
        dts        = rec['dts']
        mean_dt    = sum(dts) / len(dts)
        std_dt     = math.sqrt(
            sum((x - mean_dt) ** 2 for x in dts) / len(dts)
        ) if len(dts) > 1 else 0.0

        # Score: high frequency + close to trigger + consistent timing
        proximity  = math.exp(-abs(mean_dt) / 2.0)   # peaks at dt=0
        consistency = math.exp(-std_dt / 2.0)         # peaks at std=0
        score      = round(frequency * proximity * consistency, 4)

        # Most common transition direction
        from_vals  = [t[0] for t in rec['trans'] if t[0] is not None]
        to_vals    = [t[1] for t in rec['trans'] if t[1] is not None]
        common_from = _mode(from_vals)
        common_to   = _mode(to_vals)

        # Split key back into model / signal
        if '.' in key:
            model_part, sig_part = key.split('.', 1)
        else:
            model_part, sig_part = '', key

        results.append({
            'signal':      sig_part,
            'model':       model_part,
            'key':         key,
            'score':       score,
            'frequency':   round(frequency, 4),
            'n_episodes':  ep_count,
            'n_sorties':   len(rec['sorties']),
            'mean_dt_s':   round(mean_dt, 3),
            'std_dt_s':    round(std_dt, 3),
            'common_from': common_from,
            'common_to':   common_to,
        })

    results.sort(key=lambda r: r['score'], reverse=True)
    return results


def _mode(vals):
    if not vals:
        return None
    counts = defaultdict(int)
    for v in vals:
        counts[v] += 1
    return max(counts, key=counts.__getitem__)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description='Correlate signal transitions with fault episodes fleet-wide.')
    p.add_argument('data_root', nargs='?', default='.',
                   help='Root directory containing sortie subdirs (default: .)')
    p.add_argument('--window-pre',  type=float, default=5.0,
                   help='Seconds before trigger to include (default: 5)')
    p.add_argument('--window-post', type=float, default=1.0,
                   help='Seconds after trigger to include (default: 1)')
    p.add_argument('--min-freq', type=float, default=0.05,
                   help='Minimum episode frequency to include in output (default: 0.05)')
    p.add_argument('--top', type=int, default=100,
                   help='Max signals to include in output (default: 100)')
    p.add_argument('--sample-rate', type=float, default=40.0,
                   help='Model sample rate in Hz for dt-in-samples output (default: 40)')
    p.add_argument('--exclude', metavar='SIGNAL', nargs='+',
                   default=[
                       'enforceStandby', '*Capable', '*ActiveEnum*', 'atActCmdMode',
                       'afcsEngage*', 'afcsEngOrCws', 'fgAndAp', '*Engaged*', '*Arm*Enum*',
                       'ydEnabled', 'atCmdEng', 'apOr', 'limitMinusMargin', 'apNoMonitorTrip',
                       'atNoMonitorTrip',
                   ],
                   help='Signal suffix(es) to exclude; supports * wildcards')
    args = p.parse_args()

    data_root = os.path.abspath(args.data_root)
    exclude_patterns = list(args.exclude)

    def _is_excluded(sig):
        return any(fnmatch.fnmatch(sig, pat) for pat in exclude_patterns)

    # Find all analysis JSONs (exclude hires)
    json_paths = sorted(
        p for p in glob.glob(f'{data_root}/**/analysis_*.json', recursive=True)
        if '_hires' not in p
    )
    if not json_paths:
        sys.exit(f'No analysis_*.json found under {data_root}')

    print(f'Scanning {len(json_paths)} JSON file(s) …')

    all_episodes = []
    sortie_ep_counts = {}
    n_sorties_with_episodes = 0

    for path in json_paths:
        eps = _load_sortie(path)
        if eps:
            n_sorties_with_episodes += 1
        all_episodes.extend(eps)
        sortie_ep_counts[os.path.basename(path)] = len(eps)

    print(f'  {len(all_episodes)} episode(s) across {n_sorties_with_episodes} sortie(s)')

    if not all_episodes:
        sys.exit('No episodes found — nothing to correlate.')

    print(f'Correlating (window −{args.window_pre}s … +{args.window_post}s) …')
    correlations = _correlate(all_episodes, args.window_pre, args.window_post)

    # Filter: min frequency, excluded signals, cap at --top
    correlations = [c for c in correlations
                    if c['frequency'] >= args.min_freq
                    and not _is_excluded(c['signal'])]
    correlations = correlations[:args.top]

    # Annotate with sample-count timing
    dt_per_sample = 1.0 / args.sample_rate
    for c in correlations:
        c['mean_dt_samples'] = round(c['mean_dt_s'] / dt_per_sample, 1)
        c['std_dt_samples']  = round(c['std_dt_s']  / dt_per_sample, 1)

    out = {
        'window_pre_s':   args.window_pre,
        'window_post_s':  args.window_post,
        'sample_rate_hz': args.sample_rate,
        'n_sorties':      len(json_paths),
        'n_episodes':     len(all_episodes),
        'excluded':       exclude_patterns,
        'correlations':   correlations,
    }

    out_path = os.path.join(data_root, 'fault_correlations.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)

    print(f'  Top {min(10, len(correlations))} correlated signals:')
    for c in correlations[:10]:
        print(f'    {c["signal"]:35s}  freq={c["frequency"]:.0%}  '
              f'mean_dt={c["mean_dt_s"]:+.2f}s ({c["mean_dt_samples"]:+.1f} smp)  '
              f'score={c["score"]:.3f}')

    if exclude_patterns:
        print(f'  (excluded: {", ".join(exclude_patterns)})')
    print(f'\nWrote {out_path}  ({len(correlations)} signal(s))')


if __name__ == '__main__':
    main()
