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
        MANIFEST_ARG=$(printf '%s\n' "${MANIFEST_WIN_LIST[@]}")
        "$PYTHON" -c "
import json, sys
files = '''$MANIFEST_ARG'''.strip().splitlines()
all_sorties, base = [], None
for path in files:
    path = path.strip()
    if not path: continue
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
echo

# ── Split date range into N_CHUNKS (marker-aware) ─────────────────────────────
# Split by unmarked (data) days so each worker gets equal work, not equal
# calendar days. Falls back to calendar-day split if no markers exist yet.
MARKER_DIR_WIN="$(to_win_path "$DATA_ROOT/.pipeline_done")"
CHUNKS=$("$PYTHON" -c "
import json, datetime, os

c = json.load(open(r'$CONFIG_WIN'))
start = datetime.date.fromisoformat(c['download_start_date'])
end_raw = c['download_end_date']
end = datetime.date.today() if str(end_raw).lower() == 'today' else datetime.date.fromisoformat(end_raw)
n = $N_CHUNKS
marker_dir = r'$MARKER_DIR_WIN'

all_days = []
d = start
while d <= end:
    all_days.append(d)
    d += datetime.timedelta(days=1)

# Days without a done marker are the ones that need work
data_days = [d for d in all_days
             if not os.path.exists(os.path.join(marker_dir, d.isoformat()))]

if len(data_days) >= n:
    # Distribute data days evenly; chunk boundaries are the first/last data day
    chunk = len(data_days) // n
    for i in range(n):
        s_idx = i * chunk
        e_idx = (i + 1) * chunk - 1 if i < n - 1 else len(data_days) - 1
        print(data_days[s_idx].isoformat(), data_days[e_idx].isoformat())
else:
    # Fewer data days than chunks — fall back to equal calendar-day split
    total = len(all_days)
    chunk = max(1, total // n)
    for i in range(n):
        s = start + datetime.timedelta(days=i * chunk)
        e = start + datetime.timedelta(days=(i + 1) * chunk - 1) if i < n - 1 else end
        print(s.isoformat(), e.isoformat())
" | tr -d '\r')

# ── Launch workers ─────────────────────────────────────────────────────────────
TMPCONFIGS=()
STAGE_DIRS=()
PIDS=()
i=1
while IFS=' ' read -r chunk_start chunk_end; do
    STAGE_DIR="$DATA_ROOT/.stage/chunk_$i"
    mkdir -p "$STAGE_DIR"
    STAGE_DIRS+=("$STAGE_DIR")
    STAGE_WIN="$(to_win_path "$STAGE_DIR")"

    # Pre-populate staging with existing done markers so workers skip already-scanned days
    if [[ -d "$DATA_ROOT/.pipeline_done" ]]; then
        mkdir -p "$STAGE_DIR/.pipeline_done"
        cp "$DATA_ROOT/.pipeline_done/"* "$STAGE_DIR/.pipeline_done/" 2>/dev/null || true
    fi

    TMPCFG=$(mktemp /tmp/batch_config_chunk_XXXX.json)
    TMPCONFIGS+=("$TMPCFG")
    TMPCFG_WIN="$(to_win_path "$TMPCFG")"

    "$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
c['download_start_date'] = '$chunk_start'
c['download_end_date'] = '$chunk_end'
c['data_root'] = r'$STAGE_WIN'
with open(r'$TMPCFG_WIN', 'w') as f:
    json.dump(c, f, indent=2)
"
    LOG="/tmp/pipeline_chunk_${i}.log"
    echo "  Chunk $i: $chunk_start → $chunk_end  (log: $LOG)"
    bash "$SCRIPT_DIR/pipeline.sh" "$TMPCFG" > "$LOG" 2>&1 &
    PIDS+=($!)
    i=$(( i + 1 ))
done <<< "$CHUNKS"

echo
echo "  All $N_CHUNKS chunks running  (PIDs: ${PIDS[*]})"
echo "  Waiting for completion..."
echo

for pid in "${PIDS[@]}"; do
    wait "$pid" || true
done

echo "  All chunks finished. Merging into $DATA_ROOT ..."
echo

# ── Merge: .pipeline_done markers ─────────────────────────────────────────────
mkdir -p "$DATA_ROOT/.pipeline_done"
for stage in "${STAGE_DIRS[@]}"; do
    [[ -d "$stage/.pipeline_done" ]] || continue
    find "$stage/.pipeline_done" -maxdepth 1 -type f | while IFS= read -r marker; do
        dest="$DATA_ROOT/.pipeline_done/$(basename "$marker")"
        [[ ! -f "$dest" ]] && cp "$marker" "$dest"
    done
done

# ── Merge: sortie dirs ─────────────────────────────────────────────────────────
for stage in "${STAGE_DIRS[@]}"; do
    [[ -d "$stage" ]] || continue
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
done

# ── Merge: batch_manifest.json ─────────────────────────────────────────────────
MANIFEST_WIN_LIST=()
for stage in "${STAGE_DIRS[@]}"; do
    mf="$stage/batch_manifest.json"
    [[ -f "$mf" ]] && MANIFEST_WIN_LIST+=("$(to_win_path "$mf")")
done

if (( ${#MANIFEST_WIN_LIST[@]} > 0 )); then
    MANIFEST_OUT_WIN="$(to_win_path "$DATA_ROOT/batch_manifest.json")"
    # Build a null-delimited list safe for Python
    MANIFEST_ARG=$(printf '%s\n' "${MANIFEST_WIN_LIST[@]}")
    "$PYTHON" -c "
import json, sys
files = '''$MANIFEST_ARG'''.strip().splitlines()
all_sorties, base = [], None
for path in files:
    path = path.strip()
    if not path:
        continue
    try:
        with open(path) as f:
            m = json.load(f)
        if base is None:
            base = dict(m)
        all_sorties.extend(m.get('sorties', []))
    except Exception as e:
        print(f'  warning: {path}: {e}', file=sys.stderr)
if base:
    base['sorties'] = all_sorties
    with open(r'$MANIFEST_OUT_WIN', 'w') as f:
        json.dump(base, f, indent=2)
    print(f'  Merged {len(all_sorties)} sortie entries -> batch_manifest.json')
" || true
fi

# ── Cleanup ────────────────────────────────────────────────────────────────────
for f in "${TMPCONFIGS[@]}"; do rm -f "$f"; done
for stage in "${STAGE_DIRS[@]}"; do
    [[ -d "$stage" ]] && rm -rf "$stage" 2>/dev/null || true
done
rmdir "$DATA_ROOT/.stage" 2>/dev/null || true

MARKER_COUNT=$(find "$DATA_ROOT/.pipeline_done" -type f 2>/dev/null | wc -l)
SORTIE_COUNT=$(find "$DATA_ROOT" -maxdepth 1 -mindepth 1 -type d ! -name '.*' 2>/dev/null | wc -l)

echo "========================================================"
echo "  Parallel pipeline complete"
echo "  Sortie dirs  : $SORTIE_COUNT"
echo "  Done markers : $MARKER_COUNT"
echo "  Finished     : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Run 'python run_batch.py --status' to check analysis status."
echo "========================================================"
