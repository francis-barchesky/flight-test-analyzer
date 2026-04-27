#!/usr/bin/env python3
"""
classify_episodes.py — Port of the HTML classifyExit() logic.
Reads all analysis JSONs, classifies each AFCS disengagement episode,
and writes plots/plot_fault_dist.pdf for the proposal appendix.
"""

import glob
import json
import os
import re

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

ROOT     = os.path.dirname(os.path.abspath(__file__))
OUT      = os.path.join(ROOT, 'plots')
PRE_TRIG = 0.010   # 10 ms — matches window._preTrigMs default
AT_TRIG  = 0.05    # 50 ms — matches |dt| <= 0.05 in HTML

plt.rcParams.update({
    'font.family': 'serif',
    'font.size':   9,
    'axes.titlesize': 10,
    'axes.labelsize': 9,
})

CATEGORIES = [
    ('RESP_MONITOR',  'Response Monitor Trip',       '#C26A30'),
    ('CMD_MONITOR',   'Command Monitor Trip',         '#534AB7'),
    ('MONITOR_FAULT', 'Monitor Fault',                '#A32D2D'),
    ('VALIDITY_LOSS', 'Sensor / Validity Loss',       '#D94F4F'),
    ('MISTRIM',       'Mistrim / Torque Limit',       '#8C7A2D'),
    ('ENFORCE_SBY',   'Forced Standby',               '#5A6A8C'),
    ('CAP_LOST',      'Upstream Capability Lost',     '#6B62D4'),
    ('PILOT_CMD',     'Pilot-Commanded Disconnect',   '#2D7A4A'),
    ('UNKNOWN',       'Unknown',                      '#888780'),
]
CAT_COLOR = {k: c for k, _, c in CATEGORIES}
CAT_LABEL = {k: l for k, l, _ in CATEGORIES}


# ---------------------------------------------------------------------------
# Python port of classifyExit()
# ---------------------------------------------------------------------------

def _sfx(sig):
    return sig.split('.')[-1]

def _sfx_low(sig):
    return _sfx(sig).lower()

def _find_debounce(all_trans, axis, kind=None):
    any_re  = re.compile(r'^' + axis + r'.*debounce.*out$', re.I)
    type_re = re.compile(r'^' + axis + r'.*' + kind + r'.*debounce.*out$', re.I) if kind else None
    typed = next((c for c in all_trans if type_re and type_re.match(_sfx_low(c['signal']))), None)
    return typed or next((c for c in all_trans if any_re.match(_sfx_low(c['signal']))), None)


def classify_exit(concurrent, all_trans):
    pre  = [c for c in concurrent if c['dt'] < -PRE_TRIG]
    at   = [c for c in concurrent if abs(c['dt']) <= AT_TRIG]
    pre_at = pre + at

    # 1. Pilot-commanded disconnect
    pilot_re = re.compile(r'ap.?disc|apdisc|disconnect|^toga$|apquickdisconnect', re.I)
    pilot = next((c for c in pre
                  if pilot_re.search(_sfx_low(c['signal']))
                  and (re.search(r'ap.?disc|apdisc|disconnect', _sfx_low(c['signal']))
                       or c['to'] == 1)), None)
    if pilot:
        is_toga = bool(re.search(r'^toga$|apquickdisconnect', _sfx_low(pilot['signal'])))
        toga_engaged = not is_toga or any(
            re.search(r'afcsengage|afcsengagedcws', _sfx_low(c['signal'])) and c['from'] == 1
            for c in concurrent
        )
        if toga_engaged:
            has_mon = any(
                c['to'] == 1 and re.search(r'mon.?flag|monitorflag', _sfx_low(c['signal']))
                for c in pre_at
            )
            if not has_mon:
                lbl = 'Pilot Deactivation (TOGA/QD)' if is_toga else 'Pilot-Commanded Disconnect'
                return {'category': 'PILOT_CMD', 'label': lbl}

    # 2. Response monitor
    resp_cands = [c for c in pre_at
                  if c['to'] == 0 and re.search(r'resp.?eng', _sfx_low(c['signal']))]
    resp = next(
        (c for c in resp_cands
         if (m := re.match(r'^([a-z]+?)resp', _sfx_low(c['signal'])))
         and _find_debounce(all_trans, m.group(1), 'resp')),
        resp_cands[0] if resp_cands else None
    )
    if resp:
        return {'category': 'RESP_MONITOR', 'label': 'Response Monitor Trip'}

    # 3. Command monitor
    cmd = next((c for c in pre_at
                if c['to'] == 0 and re.search(r'cmd.?eng', _sfx_low(c['signal']))), None)
    if cmd:
        return {'category': 'CMD_MONITOR', 'label': 'Command Monitor Trip'}

    # 4. Generic monitor flag
    mon = next((c for c in pre_at
                if c['to'] == 1 and re.search(r'mon.?flag|monitorflag', _sfx_low(c['signal']))), None)
    if mon:
        return {'category': 'MONITOR_FAULT', 'label': 'Monitor Fault'}

    # 5. Mistrim / torque limit
    mis = next((c for c in pre_at
                if c['to'] == 1 and re.search(r'mistrim|xcd', _sfx_low(c['signal']))), None)
    if mis:
        return {'category': 'MISTRIM', 'label': 'Mistrim / Torque Limit'}

    # 6. Sensor / validity loss (earliest pre/at trigger)
    valid_cands = sorted(
        [c for c in pre_at
         if c['to'] == 0 and re.search(r'valid|healthy|health', _sfx_low(c['signal']))],
        key=lambda c: c['dt']
    )
    if valid_cands:
        sfx = _sfx_low(valid_cands[0]['signal'])
        src = ('ADS' if sfx.startswith('ads') else 'IRS' if sfx.startswith('irs')
               else 'PFD' if sfx.startswith('pfd') else 'FCP' if sfx.startswith('fcp')
               else 'FCC' if sfx.startswith('fcc') else None)
        lbl = (src + ' Validity Loss') if src else 'Sensor Validity Loss'
        return {'category': 'VALIDITY_LOSS', 'label': lbl}

    # 7. Enforce standby
    esb = next((c for c in pre_at
                if c['to'] == 1 and re.search(r'enforcestandby', _sfx_low(c['signal']))), None)
    if esb:
        return {'category': 'ENFORCE_SBY', 'label': 'Forced Standby'}

    # 8. Upstream capability lost
    cap = next((c for c in pre
                if c['to'] == 0 and re.search(r'capable', _sfx_low(c['signal']))), None)
    if cap:
        return {'category': 'CAP_LOST', 'label': 'Upstream Capability Lost'}

    return {'category': 'UNKNOWN', 'label': 'Unknown Exit'}


def classify_file(path):
    with open(path, encoding='utf-8') as f:
        d = json.load(f)

    trigger = d.get('trigger', {})
    trig_sig  = trigger.get('signal', 'afcsCapable').lower()
    trig_from = float(trigger.get('from', 1))
    trig_to   = float(trigger.get('to',   0))

    results = []
    for ep in d.get('episodes', []):
        trans = ep.get('transitions', [])
        if not trans:
            continue

        trig_idx = next(
            (i for i, t in enumerate(trans)
             if (t['signal'].lower().endswith('.' + trig_sig) or t['signal'].lower() == trig_sig)
             and float(t['from']) == trig_from and float(t['to']) == trig_to),
            -1
        )
        if trig_idx < 0:
            continue

        t0 = float(trans[trig_idx]['time'])

        # Build boolean concurrent array (matches JS filter)
        concurrent = []
        all_trans_mapped = []
        for i, t in enumerate(trans):
            if i == trig_idx:
                continue
            try:
                frm = float(t['from'])
                to  = float(t['to'])
                dt  = float(t['time']) - t0
            except (ValueError, TypeError, KeyError):
                continue
            mapped = {'signal': t['signal'], 'from': frm, 'to': to, 'dt': dt}
            all_trans_mapped.append(mapped)
            if frm in (0, 1) and to in (0, 1):
                concurrent.append(mapped)

        concurrent.sort(key=lambda c: abs(c['dt']))
        result = classify_exit(concurrent, all_trans_mapped)
        result['sortie'] = os.path.basename(os.path.dirname(path))
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Run across all sorties
# ---------------------------------------------------------------------------

all_results = []
json_paths = sorted(
    p for p in glob.glob(f'{ROOT}/**/analysis_*.json', recursive=True)
    if '_hires' not in p
)

for path in json_paths:
    try:
        all_results.extend(classify_file(path))
    except Exception as e:
        print(f'  skip {os.path.basename(path)}: {e}')

print(f'Classified {len(all_results)} episodes across {len(json_paths)} sorties')

# Tally
from collections import Counter
counts = Counter(r['category'] for r in all_results)
print('Distribution:')
for k, v in sorted(counts.items(), key=lambda x: -x[1]):
    print(f'  {CAT_LABEL.get(k, k):35s} {v:3d}  ({100*v/len(all_results):.0f}%)')

# ---------------------------------------------------------------------------
# Plot — horizontal bar chart sorted by count
# ---------------------------------------------------------------------------

ordered = [(k, l, c) for k, l, c in CATEGORIES if counts.get(k, 0) > 0]
ordered.sort(key=lambda x: counts[x[0]])

labels  = [l for _, l, _ in ordered]
values  = [counts[k] for k, _, _ in ordered]
colors  = [c for _, _, c in ordered]
total   = len(all_results)
n_sorties_with = len({r['sortie'] for r in all_results})

fig, ax = plt.subplots(figsize=(7.5, max(3.5, 0.45 * len(ordered) + 1.2)))
bars = ax.barh(range(len(ordered)), values, color=colors,
               edgecolor='white', linewidth=0.5, height=0.65)

for i, (bar, v) in enumerate(zip(bars, values)):
    pct = 100 * v / total
    ax.text(v + 0.3, i, f'{v}  ({pct:.0f}%)', va='center', fontsize=8)

ax.set_yticks(range(len(ordered)))
ax.set_yticklabels(labels, fontsize=8.5)
ax.set_xlabel('Number of Episodes')
ax.set_xlim(0, max(values) * 1.28)
ax.set_title(
    f'AFCS Disengagement Episode Classification — N208B 2026 Flight Testing\n'
    f'{total} episodes across {n_sorties_with} sorties',
    fontsize=9
)
ax.grid(axis='x', linestyle='--', linewidth=0.4, alpha=0.5)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'plot_fault_dist.pdf'), bbox_inches='tight')
plt.close(fig)
print(f'\nWrote plots/plot_fault_dist.pdf')
