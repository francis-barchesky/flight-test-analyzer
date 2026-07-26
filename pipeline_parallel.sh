#!/usr/bin/env bash
# pipeline_parallel.sh — Download + analyze in N parallel date-range chunks.
# Builds S3 listing cache once, shares it across all workers.
# Each worker uses an isolated staging dir to avoid download conflicts.
# After all workers finish, merges sortie dirs + markers + manifest into data_root.
#
# Usage:
#   bash pipeline_parallel.sh                    # 8 chunks, uses batch_config.json
#   bash pipeline_parallel.sh --chunks=4
#   bash pipeline_parallel.sh my_config.json
#   bash pipeline_parallel.sh --merge-only       # salvage completed work from .stage dirs
set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
N_CHUNKS=8
MERGE_ONLY=0

for arg in "$@"; do
    case "$arg" in
        *.json)      CONFIG="$(realpath "$arg")" ;;
        --chunks=*)  N_CHUNKS="${arg#--chunks=}" ;;
        --merge-only) MERGE_ONLY=1 ;;
    esac
done

PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
PYTHON="$PY313"

to_win_path() {
    if command -v cygpath &>/dev/null; then
        cygpath -w "$1"
    else
        echo "$1" | sed 's|^/\([a-zA-Z]\)/|\1:/|'
    fi
}

CONFIG_WIN="$(to_win_path "$CONFIG")"

# ── Resolve real DATA_ROOT ─────────────────────────────────────────────────────
DATA_ROOT=$("$PYTHON" -c "
import json, os
c = json.load(open(r'$CONFIG_WIN'))
d = c.get('data_root', '.')
print(d)
" | tr -d '\r')
[[ "$DATA_ROOT" == /* ]] || DATA_ROOT="$SCRIPT_DIR/$DATA_ROOT"
mkdir -p "$DATA_ROOT"
DATA_ROOT="$(cd "$DATA_ROOT" && pwd)"

echo "========================================================"
if [[ $MERGE_ONLY -eq 1 ]]; then
echo "  Parallel pipeline  (merge-only)"
else
echo "  Parallel pipeline  ($N_CHUNKS chunks)"
fi
echo "  config    : $CONFIG"
echo "  data_root : $DATA_ROOT"
echo "  started   : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
echo

# ── Merge-only: skip straight to merge ────────────────────────────────────────
if [[ $MERGE_ONLY -eq 1 ]]; then
    if [[ ! -d "$DATA_ROOT/.stage" ]]; then
        echo "  No .stage directory found — nothing to merge."
        exit 0
    fi
    echo "  Merging completed staging dirs into $DATA_ROOT ..."
    echo

    _do_merge() {
        local DATA_ROOT="$1"
        local MANIFEST_WIN_LIST=()
        while IFS= read -r stage; do
            [[ -d "$stage" ]] || continue
            # Copy .pipeline_done markers
            if [[ -d "$stage/.pipeline_done" ]]; then
                mkdir -p "$DATA_ROOT/.pipeline_done"
                find "$stage/.pipeline_done" -maxdepth 1 -type f | while IFS= read -r marker; do
                    dest="$DATA_ROOT/.pipeline_done/$(basename "$marker")"
                    [[ ! -f "$dest" ]] && cp "$marker" "$dest"
                done
            fi
            # Move sortie dirs
            while IFS= read -r entry; do
                name=$(basename "$entry")
                dest="$DATA_ROOT/$name"
                if [[ -d "$dest" ]]; then
                    find "$entry" -maxdepth 1 -type f -exec mv -f {} "$dest/" \;
                    rmdir "$entry" 2>/dev/null || true
                else
                    mv "$entry" "$dest"
                fi
            done < <(find "$stage" -maxdepth 1 -mindepth 1 -type d ! -name '.*' 2>/dev/null)
            [[ -f "$stage/batch_manifest.json" ]] && MANIFEST_WIN_LIST+=("$(to_win_path "$stage/batch_manifest.json")")
        done < <(find "$DATA_ROOT/.stage" -maxdepth 1 -mindepth 1 -type d 2>/dev/null)
        echo "${MANIFEST_WIN_LIST[@]:-}"
    }

    mapfile -t MANIFEST_WIN_LIST < <(_do_merge "$DATA_ROOT" | tr ' ' '\n' | grep -v '^$' || true)

    if (( ${#MANIFEST_WIN_LIST[@]} > 0 )); then
        MANIFEST_OUT_WIN="$(to_win_path "$DATA_ROOT/batch_manifest.json")"
        MANIFEST_LIST_TMP=$(mktemp)
        printf '%s\n' "${MANIFEST_WIN_LIST[@]}" > "$MANIFEST_LIST_TMP"
        MANIFEST_LIST_TMP_WIN="$(to_win_path "$MANIFEST_LIST_TMP")"
        "$PYTHON" -c "
import json, sys
with open(r'$MANIFEST_LIST_TMP_WIN') as flist:
    files = [l.strip() for l in flist if l.strip()]
all_sorties, base = [], None
for path in files:
    try:
        with open(path) as f: m = json.load(f)
        if base is None: base = dict(m)
        all_sorties.extend(m.get('sorties', []))
    except Exception as e:
        print(f'  warning: {path}: {e}', file=sys.stderr)
if base:
    base['sorties'] = all_sorties
    with open(r'$MANIFEST_OUT_WIN', 'w') as f: json.dump(base, f, indent=2)
    print(f'  Merged {len(all_sorties)} sortie entries -> batch_manifest.json')
" || true
        rm -f "$MANIFEST_LIST_TMP"
    fi

    MARKER_COUNT=$(find "$DATA_ROOT/.pipeline_done" -type f 2>/dev/null | wc -l)
    SORTIE_COUNT=$(find "$DATA_ROOT" -maxdepth 1 -mindepth 1 -type d ! -name '.*' 2>/dev/null | wc -l)
    echo "========================================================"
    echo "  Merge complete"
    echo "  Sortie dirs  : $SORTIE_COUNT"
    echo "  Done markers : $MARKER_COUNT"
    echo "  Finished     : $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  .stage dirs left in place — delete manually when ready:"
    echo "    rm -rf $DATA_ROOT/.stage"
    echo "========================================================"
    exit 0
fi

# ── AWS SSO check ──────────────────────────────────────────────────────────────
echo "  Checking AWS SSO login..."
if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS SSO session expired. Run: aws sso login"
    exit 1
fi
echo "  AWS SSO OK"
echo

# ── S3 listing cache ───────────────────────────────────────────────────────────
EXPORTS_CACHE="/tmp/iads_ls_exports_cache.txt"
ANALYSIS_CACHE="/tmp/iads_ls_analysis_cache.txt"
CACHE_MAX_AGE=14400

_cache_fresh() {
    local f=$1
    [[ -f "$f" ]] || return 1
    local now mtime
    now=$(date +%s)
    mtime=$(stat -c %Y "$f" 2>/dev/null || echo 0)
    (( now - mtime < CACHE_MAX_AGE ))
}

if _cache_fresh "$EXPORTS_CACHE"; then
    echo "  S3 exports listing : reusing cache  ($(wc -l < "$EXPORTS_CACHE") entries)"
else
    echo "  S3 exports listing : fetching..."
    aws s3 ls s3://merlin-pilot-iads-data-exports --recursive > "$EXPORTS_CACHE"
    echo "  S3 exports listing : $(wc -l < "$EXPORTS_CACHE") entries cached"
fi
if _cache_fresh "$ANALYSIS_CACHE"; then
    echo "  S3 analysis listing: reusing cache  ($(wc -l < "$ANALYSIS_CACHE") entries)"
else
    echo "  S3 analysis listing: fetching..."
    aws s3 ls s3://merlin-pilot-iads-analysis --recursive > "$ANALYSIS_CACHE"
    echo "  S3 analysis listing: $(wc -l < "$ANALYSIS_CACHE") entries cached"
fi
export IADS_EXPORTS_LISTING="$EXPORTS_CACHE"
export IADS_ANALYSIS_LISTING="$ANALYSIS_CACHE"
EXPORTS_CACHE_WIN="$(to_win_path "$EXPORTS_CACHE")"
echo

# ── Split by sortie name (round-robin across chunks) ──────────────────────────
# Extract unique sortie names from the S3 listing, assign each sortie to exactly
# one chunk via round-robin. Each chunk gets a filtered listing so it only
# downloads/analyzes its own sorties, eliminating the race where multiple chunks
# pick up the same sortie from overlapping date ranges.
# --no-empty-markers is passed so chunks don't falsely mark days empty when
# another chunk's sorties are the only ones with S3 data on that day.
DATA_ROOT_WIN="$(to_win_path "$DATA_ROOT")"
SCRIPT_DIR_WIN="$(to_win_path "$SCRIPT_DIR")"

SORTIE_SPLIT_FILE="/tmp/pipeline_sortie_split_$$.txt"
"$PYTHON" -c "
import json, os, re, glob, sys
from collections import defaultdict

def sortie_from_line(line):
    parts = line.strip().split()
    if len(parts) < 4: return None
    name = os.path.splitext(os.path.basename(parts[-1]))[0]
    m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})([A-Z][A-Z0-9]+)_(\d{1,3})(?!\d)', name, re.IGNORECASE)
    if m: return f'{m.group(1).upper()}_{m.group(3)}_{m.group(2).upper()}'
    m = re.search(r'(?<![A-Za-z])([SG]\d{2,5})([A-Z][A-Z0-9]+)', name, re.IGNORECASE)
    if m: return f'{m.group(1).upper()}_{m.group(2).upper()}'
    return None

c = json.load(open(r'$CONFIG_WIN'))
listing_path = r'$EXPORTS_CACHE_WIN'
script_dir = r'$SCRIPT_DIR_WIN'
n = $N_CHUNKS
exclude = c.get('exclude_zip_patterns', [])
skip_existing = c.get('skip_existing', False)
tail_normalize = c.get('tail_normalize_map', {})

# Resolve data_root_map: tail -> absolute path
def resolve_dr(path):
    if os.path.isabs(path): return path
    return os.path.normpath(os.path.join(script_dir, path))

raw_map = c.get('data_root_map', {})
data_root_map = {tail: resolve_dr(dr) for tail, dr in raw_map.items()}
default_data_root = r'$DATA_ROOT_WIN'

def normalize_sortie(s):
    parts = s.rsplit('_', 1)
    if len(parts) == 2 and parts[1] in tail_normalize:
        return parts[0] + '_' + tail_normalize[parts[1]]
    return s

def sortie_tail(s):
    return s.rsplit('_', 1)[-1]

def sortie_data_root(s):
    return data_root_map.get(sortie_tail(s), default_data_root)

zip_count = {}  # sortie -> number of matching listing lines
with open(listing_path) as f:
    for line in f:
        s = sortie_from_line(line)
        if s:
            s = normalize_sortie(s)
            zip_count[s] = zip_count.get(s, 0) + 1

sorties = sorted(s for s in zip_count
                 if not any(re.search(p, s, re.IGNORECASE) for p in exclude))

if skip_existing:
    sorties = [s for s in sorties
               if not (glob.glob(os.path.join(sortie_data_root(s), s, 'analysis*.json')) or
                       glob.glob(os.path.join(sortie_data_root(s), s, 'analysis*.json.gz')))]

# Group by tail, allocate chunks proportionally (min 1 per tail)
tail_groups = defaultdict(list)
for s in sorties:
    tail_groups[sortie_tail(s)].append(s)

total = len(sorties)
chunk_lists = []
chunk_tails = []

for tail in sorted(tail_groups.keys()):
    tail_sorties = tail_groups[tail]
    n_for_tail = max(1, round(n * len(tail_sorties) / total)) if total > 0 else 1

    tc_lists = [[] for _ in range(n_for_tail)]
    tc_weights = [0] * n_for_tail
    for s in sorted(tail_sorties, key=lambda s: zip_count.get(s, 1), reverse=True):
        lightest = min(range(n_for_tail), key=lambda i: tc_weights[i])
        tc_lists[lightest].append(s)
        tc_weights[lightest] += zip_count.get(s, 1)

    for cl, w in zip(tc_lists, tc_weights):
        chunk_lists.append(cl)
        chunk_tails.append(tail)
        print(f'  {tail} chunk {len([x for x in chunk_tails if x == tail])}: '
              f'{len(cl)} sortie(s), {w} ZIP(s)', file=sys.stderr)

total_zips = sum(zip_count.get(s, 1) for s in sorties)
n_active = sum(1 for cl in chunk_lists if cl)
print(f'  {total} sortie(s) -> {n_active} active chunk(s)  '
      f'(skip_existing={skip_existing}, total_zips={total_zips})')

# Output: CHUNK:TAIL:sortie1,sortie2,...
for tail, chunk in zip(chunk_tails, chunk_lists):
    print(f'CHUNK:{tail}:' + ','.join(chunk))
" | tr -d '\r' | tee "$SORTIE_SPLIT_FILE"
readarray -t CHUNK_SORTIES < <(grep '^CHUNK:' "$SORTIE_SPLIT_FILE" | sed 's/^CHUNK:[^:]*://' | tr -d '\r')
readarray -t CHUNK_TAILS   < <(grep '^CHUNK:' "$SORTIE_SPLIT_FILE" | sed 's/^CHUNK:\([^:]*\):.*/\1/' | tr -d '\r')

# ── Launch workers ─────────────────────────────────────────────────────────────
TMPCONFIGS=()
TMPLISTINGS=()
STAGE_DIRS=()
CHUNK_DATA_ROOTS=()
PIDS=()
i=1
for CHUNK_SORTIES_STR in "${CHUNK_SORTIES[@]}"; do
    [[ -z "$CHUNK_SORTIES_STR" ]] && { i=$(( i + 1 )); continue; }

    CHUNK_TAIL="${CHUNK_TAILS[$((i-1))]:-}"
    CHUNK_DATA_ROOT=$("$PYTHON" -c "
import json, os
c = json.load(open(r'$CONFIG_WIN'))
m = c.get('data_root_map', {})
tail = '$CHUNK_TAIL'
dr = m.get(tail, c.get('data_root', '.'))
script_dir = r'$SCRIPT_DIR_WIN'
if not os.path.isabs(dr): dr = os.path.normpath(os.path.join(script_dir, dr))
print(dr)
" | tr -d '\r')
    mkdir -p "$CHUNK_DATA_ROOT"
    CHUNK_DATA_ROOT="$(cd "$CHUNK_DATA_ROOT" && pwd)"
    CHUNK_DATA_ROOTS+=("$CHUNK_DATA_ROOT")

    STAGE_DIR="$CHUNK_DATA_ROOT/.stage/chunk_$i"
    mkdir -p "$STAGE_DIR"
    STAGE_DIRS+=("$STAGE_DIR")
    STAGE_WIN="$(to_win_path "$STAGE_DIR")"

    # Pre-populate staging with existing done markers so workers skip already-scanned days
    if [[ -d "$CHUNK_DATA_ROOT/.pipeline_done" ]]; then
        mkdir -p "$STAGE_DIR/.pipeline_done"
        cp "$CHUNK_DATA_ROOT/.pipeline_done/"* "$STAGE_DIR/.pipeline_done/" 2>/dev/null || true
    fi

    # Build per-chunk filtered S3 listings (exports + analysis, only this chunk's sorties)
    FILTERED_LISTING="/tmp/pipeline_chunk_${i}_listing.txt"
    FILTERED_ANALYSIS_LISTING="/tmp/pipeline_chunk_${i}_analysis_listing.txt"
    TMPLISTINGS+=("$FILTERED_LISTING" "$FILTERED_ANALYSIS_LISTING")
    FILTERED_WIN="$(to_win_path "$FILTERED_LISTING")"
    FILTERED_ANALYSIS_WIN="$(to_win_path "$FILTERED_ANALYSIS_LISTING")"
    ANALYSIS_CACHE_WIN="$(to_win_path "$ANALYSIS_CACHE")"
    CHUNK_SORTIE_COUNT=$(echo "$CHUNK_SORTIES_STR" | tr ',' '\n' | grep -c . || true)
    "$PYTHON" -c "
import os, re, sys, json

def sortie_to_pattern(sortie):
    parts = sortie.split('_')
    if len(parts) == 3:
        prefix, leg, tail = parts
        return rf'{prefix}{tail}_{leg}(?!\d)'
    elif len(parts) == 2:
        prefix, tail = parts
        return rf'{prefix}{tail}(?!_\d)'
    return re.escape(sortie)

c = json.load(open(r'$CONFIG_WIN'))
tail_normalize = c.get('tail_normalize_map', {})
# Build reverse map: canonical tail -> list of alias tails
tail_aliases = {}
for alias, canonical in tail_normalize.items():
    tail_aliases.setdefault(canonical, []).append(alias)

def sortie_alias_patterns(sortie):
    parts = sortie.split('_')
    tail = parts[-1] if len(parts) >= 2 else None
    aliases = tail_aliases.get(tail, []) if tail else []
    pats = [sortie_to_pattern(sortie)]
    for alias_tail in aliases:
        alias_sortie = '_'.join(parts[:-1] + [alias_tail])
        pats.append(sortie_to_pattern(alias_sortie))
    return pats

sorties = [s.strip() for s in '${CHUNK_SORTIES_STR}'.split(',') if s.strip()]
patterns = [re.compile(p, re.IGNORECASE)
            for s in sorties for p in sortie_alias_patterns(s)]

for src, dst in [(r'$EXPORTS_CACHE_WIN', r'$FILTERED_WIN'),
                 (r'$ANALYSIS_CACHE_WIN', r'$FILTERED_ANALYSIS_WIN')]:
    with open(src) as fin, open(dst, 'w') as fout:
        for line in fin:
            if any(p.search(line) for p in patterns):
                fout.write(line)
"

    # Config with full date range, pointing to this chunk's staging dir
    TMPCFG=$(mktemp /tmp/batch_config_chunk_XXXX.json)
    TMPCONFIGS+=("$TMPCFG")
    TMPCFG_WIN="$(to_win_path "$TMPCFG")"
    "$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
c['data_root'] = r'$STAGE_WIN'
with open(r'$TMPCFG_WIN', 'w') as f:
    json.dump(c, f, indent=2)
"

    LOG="/tmp/pipeline_chunk_${i}.log"
    echo "  Chunk $i: $CHUNK_SORTIE_COUNT sortie(s)  (log: $LOG)"
    { echo "# chunk_sorties: $CHUNK_SORTIE_COUNT"; echo "# chunk_stage_dir: $STAGE_WIN"; } > "$LOG"
    IADS_EXPORTS_LISTING="$FILTERED_LISTING" \
    IADS_ANALYSIS_LISTING="$FILTERED_ANALYSIS_LISTING" \
        bash "$SCRIPT_DIR/pipeline.sh" "$TMPCFG" --no-empty-markers >> "$LOG" 2>&1 &
    PIDS+=($!)
    i=$(( i + 1 ))
done

echo
echo "  ${#PIDS[@]} chunk(s) running  (PIDs: ${PIDS[*]})"
echo "  Waiting for completion..."
echo

for pid in "${PIDS[@]}"; do
    wait "$pid" || true
done

echo "  All chunks finished. Merging into $DATA_ROOT ..."
echo

# ── Merge: .pipeline_done markers ─────────────────────────────────────────────
for j in "${!STAGE_DIRS[@]}"; do
    stage="${STAGE_DIRS[$j]}"
    cdr="${CHUNK_DATA_ROOTS[$j]}"
    [[ -d "$stage/.pipeline_done" ]] || continue
    mkdir -p "$cdr/.pipeline_done"
    while IFS= read -r marker; do
        dest="$cdr/.pipeline_done/$(basename "$marker")"
        if [[ ! -f "$dest" ]]; then cp "$marker" "$dest"; fi
    done < <(find "$stage/.pipeline_done" -maxdepth 1 -type f 2>/dev/null)
done

# ── Merge: sortie dirs ─────────────────────────────────────────────────────────
for j in "${!STAGE_DIRS[@]}"; do
    stage="${STAGE_DIRS[$j]}"
    cdr="${CHUNK_DATA_ROOTS[$j]}"
    [[ -d "$stage" ]] || continue
    while IFS= read -r entry; do
        name=$(basename "$entry")
        dest="$cdr/$name"
        if [[ -d "$dest" ]]; then
            find "$entry" -maxdepth 1 -type f -exec mv -f {} "$dest/" \;
            rmdir "$entry" 2>/dev/null || true
        else
            mv "$entry" "$dest"
        fi
    done < <(find "$stage" -maxdepth 1 -mindepth 1 -type d ! -name '.*' 2>/dev/null)
done

# ── Merge: batch_manifest.json ─────────────────────────────────────────────────
declare -A MANIFEST_BY_DR
for j in "${!STAGE_DIRS[@]}"; do
    stage="${STAGE_DIRS[$j]}"
    cdr="${CHUNK_DATA_ROOTS[$j]}"
    mf="$stage/batch_manifest.json"
    [[ -f "$mf" ]] && MANIFEST_BY_DR["$cdr"]+="$(to_win_path "$mf")"$'\n'
done

for cdr in "${!MANIFEST_BY_DR[@]}"; do
    MANIFEST_ARG="${MANIFEST_BY_DR[$cdr]}"
    MANIFEST_OUT_WIN="$(to_win_path "$cdr/batch_manifest.json")"
    MANIFEST_LIST_TMP=$(mktemp)
    printf '%s' "$MANIFEST_ARG" > "$MANIFEST_LIST_TMP"
    MANIFEST_LIST_TMP_WIN="$(to_win_path "$MANIFEST_LIST_TMP")"
    "$PYTHON" -c "
import json, sys
with open(r'$MANIFEST_LIST_TMP_WIN') as flist:
    files = [l.strip() for l in flist if l.strip()]
all_sorties, base = [], None
for path in files:
    try:
        with open(path) as f: m = json.load(f)
        if base is None: base = dict(m)
        all_sorties.extend(m.get('sorties', []))
    except Exception as e:
        print(f'  warning: {path}: {e}', file=sys.stderr)
if base:
    base['sorties'] = all_sorties
    with open(r'$MANIFEST_OUT_WIN', 'w') as f: json.dump(base, f, indent=2)
    print(f'  Merged {len(all_sorties)} sortie entries -> batch_manifest.json')
" || true
    rm -f "$MANIFEST_LIST_TMP"
done

# ── Cleanup ────────────────────────────────────────────────────────────────────
for f in "${TMPCONFIGS[@]}"; do rm -f "$f"; done
for f in "${TMPLISTINGS[@]}"; do rm -f "$f"; done
rm -f "/tmp/pipeline_sortie_split_$$.txt"
for stage in "${STAGE_DIRS[@]}"; do
    [[ -d "$stage" ]] && rm -rf "$stage" 2>/dev/null || true
done
# Remove .stage dirs per unique data root
declare -A SEEN_CDR
for cdr in "${CHUNK_DATA_ROOTS[@]}"; do
    [[ -n "${SEEN_CDR[$cdr]:-}" ]] && continue
    SEEN_CDR["$cdr"]=1
    rmdir "$cdr/.stage" 2>/dev/null || true
done

# Per-tail summary
declare -A TAIL_MARKERS TAIL_SORTIES
for cdr in "${!SEEN_CDR[@]}"; do
    tail=$(basename "$cdr")
    TAIL_MARKERS[$tail]=$(find "$cdr/.pipeline_done" -type f 2>/dev/null | wc -l)
    TAIL_SORTIES[$tail]=$(find "$cdr" -maxdepth 1 -mindepth 1 -type d ! -name '.*' 2>/dev/null | wc -l)
done

echo "========================================================"
echo "  Parallel pipeline complete"
for tail in "${!TAIL_MARKERS[@]}"; do
    echo "  $tail — sortie dirs: ${TAIL_SORTIES[$tail]}  markers: ${TAIL_MARKERS[$tail]}"
done
echo "  Finished     : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
