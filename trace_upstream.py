#!/usr/bin/env python3
"""
trace_upstream.py — Walk the model trace graph upstream from a target signal
and report observable (TestPoint) signals with hop counts, models, and
optional cross-reference against fault_correlations.json.

Usage:
    python trace_upstream.py --signal afcsCapable [data_root]
    python trace_upstream.py --signal afcsCapable --trace path/to/traceData.json .
    python trace_upstream.py --signal afcsCapable --max-hops 20 .
"""

import argparse
import glob as _glob
import json
import os
import re
import sys
from collections import defaultdict, deque


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------

def _load_graph(path):
    print(f'Loading trace graph: {os.path.basename(path)} ...')
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    nodes = {n['key']: n for n in d['nodes']}

    # Build reverse adjacency: dstKey -> [srcKey]  (upstream direction)
    rev = defaultdict(list)
    for e in d['edges']:
        rev[e['dstKey']].append(e['srcKey'])

    # Name index: blockName -> [keys]
    name_idx = defaultdict(list)
    for k, n in nodes.items():
        bn = n.get('blockName', '')
        if bn:
            name_idx[bn].append(k)

    # Set of all model names for computing-model lookup
    all_models = set(n.get('model', '') for n in nodes.values())
    all_models.discard('')

    stats = d.get('stats', {})
    print(f'  {stats.get("totalNodes", len(nodes)):,} nodes, '
          f'{stats.get("totalEdges", "?"):,} edges, '
          f'{len(all_models):,} models')
    return nodes, rev, name_idx, all_models


# ---------------------------------------------------------------------------
# Start-node discovery
# ---------------------------------------------------------------------------

def _find_start_keys(signal, nodes, rev, name_idx, *, model_only=False):
    """
    Find the computation node(s) for `signal`.

    Strategy:
    1. Find the model named after the signal (e.g. AfcsCapable for afcsCapable).
       Within that model, find Logic/SubSystem Outport:1 nodes whose KEY PATH
       contains the signal name — this is the output AND/OR gate.
    2. Fall back to the highest-incoming Logic Outport:1 node in that model.
    3. Final fallback (skipped when model_only=True): any node with blockName or
       testPointName == signal that has incoming edges.
    """
    target_model = signal[0].upper() + signal[1:]
    signal_lower = signal.lower()

    named_gate = None   # key path contains signal name
    best_gate = None    # fallback: most incoming Logic Outport:1

    for k, n in nodes.items():
        if n.get('model') != target_model:
            continue
        if 'Outport:1' not in k:
            continue
        if n.get('blockType') not in ('Logic', 'SubSystem', 'SignalConversion'):
            continue
        count = len(rev.get(k, []))
        if count == 0:
            continue
        parts = k.split('/')
        parent = parts[-2].lower() if len(parts) >= 2 else ''
        if signal_lower in parent and (named_gate is None or
                count > len(rev.get(named_gate, []))):
            named_gate = k
        if best_gate is None or count > len(rev.get(best_gate, [])):
            best_gate = k

    start = named_gate or best_gate
    if start:
        return [start]

    if model_only:
        return []

    # Fallback: any node with matching blockName / testPointName with incoming edges
    candidates = []
    for k in name_idx.get(signal, []):
        if rev.get(k):
            candidates.append(k)
    for k, n in nodes.items():
        if n.get('testPointName') == signal and rev.get(k):
            candidates.append(k)

    return list(set(candidates))


# ---------------------------------------------------------------------------
# BFS upstream traversal
# ---------------------------------------------------------------------------

def _bfs_upstream(start_keys, nodes, rev, name_idx, all_models, max_hops):
    """
    BFS following incoming edges (upstream direction).

    Dead-end Inport nodes (blockType='Inport', no incoming) are resolved via:
      1. Cross-model jump: if a model named after blockName exists in the graph,
         jump to its computation output (the AND/OR gate in that model).
         This correctly handles the case where AfcsCapable receives apCapable
         from model ApCapable — we skip intra-model heuristics that would
         wrongly stay inside AfcsCapable.
      2. Same-model {bn}Logic subsystem heuristic (for locally-computed signals
         that have no dedicated top-level model).
      3. Generic blockName fallback.

    Returns list of dicts for each unique TestPoint blockName found, with
    the minimum hop count.
    """
    visited = {}      # key -> hop
    queue = deque()

    for k in start_keys:
        queue.append((k, 0))
        visited[k] = 0

    # Results: blockName -> best (hop, node_dict, key)
    results = {}

    while queue:
        key, hop = queue.popleft()
        node = nodes.get(key)
        if not node:
            continue

        bn = node.get('blockName', '')

        # Record TestPoint hits (not the root itself at hop 0)
        # Suppress signalConversion* — Simulink type-cast blocks, not diagnostic signals
        if (hop > 0 and node.get('isTestPoint') and bn
                and not bn.startswith('signalConversion')):
            if bn not in results or hop < results[bn]['hop']:
                results[bn] = {
                    'signal':    bn,
                    'model':     node.get('model', ''),
                    'blockType': node.get('blockType', ''),
                    'hop':       hop,
                    'key':       key,
                }

        if hop >= max_hops:
            continue

        src_keys = rev.get(key, [])

        # Dead-end Inport with no upstream: try to cross model/subsystem boundary
        if not src_keys and node.get('blockType') == 'Inport' and bn:
            model = node.get('model', '')

            # (1) Cross-model: jump to the model that COMPUTES this signal.
            # Prioritised over the {bn}Logic heuristic to avoid incorrectly
            # routing through a same-named subsystem inside the current model
            # (e.g. AfcsCapable/apCapableLogic) when the signal is actually
            # produced by a peer model (ApCapable).
            target_model = bn[0].upper() + bn[1:]
            if target_model in all_models:
                cross = _find_start_keys(bn, nodes, rev, name_idx, model_only=True)
                if cross:
                    src_keys = cross

            # (2) Same-model {bn}Logic subsystem (for signals without own model)
            if not src_keys:
                logic_pattern = bn.lower() + 'logic'
                same_model_candidates = [
                    k2 for k2, n2 in nodes.items()
                    if n2.get('model') == model
                    and 'Outport:1' in k2
                    and logic_pattern in k2.lower()
                    and rev.get(k2)
                ]
                if same_model_candidates:
                    same_model_candidates.sort(
                        key=lambda k2: len(rev.get(k2, [])), reverse=True)
                    src_keys = [same_model_candidates[0]]

            # (3) Fallback: same blockName with incoming edges in any model
            if not src_keys:
                src_keys = [
                    k2 for k2 in name_idx.get(bn, [])
                    if k2 != key and rev.get(k2)
                ]

        for src in src_keys:
            if src not in visited:
                visited[src] = hop + 1
                queue.append((src, hop + 1))

    return list(results.values())


# ---------------------------------------------------------------------------
# Correlations cross-reference
# ---------------------------------------------------------------------------

def _load_correlations(data_root):
    path = os.path.join(data_root, 'fault_correlations.json')
    if not os.path.isfile(path):
        return {}
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    return {c['signal']: c for c in d.get('correlations', [])}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description='Trace upstream dependencies of a signal through the model graph.')
    p.add_argument('data_root', nargs='?', default='.',
                   help='Directory containing fault_correlations.json (default: .)')
    p.add_argument('--signal', required=True,
                   help='Target signal name (e.g. afcsCapable)')
    p.add_argument('--trace', metavar='JSON',
                   help='Path to traceData JSON (auto-discovered if omitted)')
    p.add_argument('--max-hops', type=int, default=15,
                   help='Maximum hops upstream to traverse (default: 15)')
    p.add_argument('--top', type=int, default=60,
                   help='Max results to show (default: 60)')
    args = p.parse_args()

    # Find trace file
    if args.trace:
        trace_path = args.trace
    else:
        candidates = sorted(_glob.glob(
            str(os.path.join(os.path.dirname(__file__), '..', 'trace-analyzer',
                             '**', 'traceData*.json')),
            recursive=True
        ), reverse=True)
        if not candidates:
            candidates = sorted(_glob.glob('**/traceData*.json', recursive=True), reverse=True)
        if not candidates:
            sys.exit('No traceData*.json found. Use --trace <path>.')
        trace_path = candidates[0]
        print(f'Using trace: {trace_path}')

    nodes, rev, name_idx, all_models = _load_graph(trace_path)
    correlations = _load_correlations(args.data_root)
    if correlations:
        print(f'Loaded {len(correlations)} fault correlations.')

    # Find start node(s)
    start_keys = _find_start_keys(args.signal, nodes, rev, name_idx)
    if not start_keys:
        sys.exit(f'Could not find computation node for signal "{args.signal}". '
                 f'Try --signal with exact blockName.')

    print(f'\nStarting from {len(start_keys)} node(s) for "{args.signal}":')
    for k in start_keys[:3]:
        print(f'  {k}  (incoming={len(rev.get(k,[]))})')

    # BFS
    print(f'\nTraversing upstream (max {args.max_hops} hops) ...')
    results = _bfs_upstream(start_keys, nodes, rev, name_idx, all_models, args.max_hops)
    results.sort(key=lambda r: (r['hop'], r['signal']))

    # Annotate with correlations
    for r in results:
        r['corr'] = correlations.get(r['signal'])

    # Console output
    print()
    corr_count = sum(1 for r in results if r['corr'])
    print(f'{"Signal":<40} {"Hops":>5}  {"Model":<35}  Correlation')
    print(f'{"-"*40} {"-----":>5}  {"-"*35}  -----------')
    for r in results[:args.top]:
        c = r['corr']
        corr_str = (
            f'freq={c["frequency"]:.0%}  '
            f'dt={c["mean_dt_s"]:+.2f}s ({c.get("mean_dt_samples","?"):+.1f} smp)  '
            f'score={c["score"]:.3f}'
        ) if c else ''
        print(f'  {r["signal"]:<38} {r["hop"]:>5}  {r["model"]:<35}  {corr_str}')

    print()
    print(f'{len(results)} upstream TestPoint signal(s) found '
          f'({corr_count} appear in fault correlations).')

    # Write output JSON
    out = {
        'signal':     args.signal,
        'max_hops':   args.max_hops,
        'trace_file': os.path.basename(trace_path),
        'n_upstream': len(results),
        'upstream': [
            {
                'signal':             r['signal'],
                'model':              r['model'],
                'hop':                r['hop'],
                'blockType':          r['blockType'],
                'corr_frequency':     r['corr']['frequency']     if r['corr'] else None,
                'corr_mean_dt_s':     r['corr']['mean_dt_s']     if r['corr'] else None,
                'corr_mean_dt_smp':   r['corr'].get('mean_dt_samples') if r['corr'] else None,
                'corr_score':         r['corr']['score']         if r['corr'] else None,
            }
            for r in results
        ],
    }
    out_path = os.path.join(args.data_root, f'upstream_{args.signal}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)
    print(f'Wrote {out_path}')


if __name__ == '__main__':
    main()
