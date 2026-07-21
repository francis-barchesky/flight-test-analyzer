#!/usr/bin/env bash
# scan_parallel.sh — Pre-populate empty-day markers by scanning S3 in parallel chunks.
# Run this once before pipeline.sh to make subsequent runs skip empty days instantly.
#
# Usage:
#   bash scan_parallel.sh              # uses batch_config.json, 16 chunks
#   bash scan_parallel.sh --chunks=8   # fewer chunks
#   bash scan_parallel.sh my_config.json
set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
N_CHUNKS=16

for arg in "$@"; do
    case "$arg" in
        *.json)     CONFIG="$(realpath "$arg")" ;;
        --chunks=*) N_CHUNKS="${arg#--chunks=}" ;;
    esac
done

PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
PYTHON="$PY313"

# Convert /c/... or /mnt/c/... path to C:/... for Windows Python
to_win_path() {
    if command -v cygpath &>/dev/null; then
        cygpath -w "$1"
    else
        echo "$1" | sed 's|^/\([a-zA-Z]\)/|\1:/|'
    fi
}

CONFIG_WIN="$(to_win_path "$CONFIG")"

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

echo "  Checking AWS SSO login..."
if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS SSO session expired. Run: aws sso login"
    exit 1
fi
echo "  AWS SSO OK"

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

# ── Split date range into N_CHUNKS ────────────────────────────────────────────
CHUNKS=$("$PYTHON" -c "
import json, datetime
c = json.load(open(r'$CONFIG_WIN'))
start = datetime.date.fromisoformat(c['download_start_date'])
end_raw = c['download_end_date']
end = datetime.date.today() if str(end_raw).lower() == 'today' else datetime.date.fromisoformat(end_raw)
total = (end - start).days + 1
n = $N_CHUNKS
chunk = total // n
for i in range(n):
    s = start + datetime.timedelta(days=i * chunk)
    e = start + datetime.timedelta(days=(i + 1) * chunk - 1) if i < n - 1 else end
    print(s.isoformat(), e.isoformat())
" | tr -d '\r')

echo "========================================================"
echo "  Parallel scan  ($N_CHUNKS chunks)"
echo "  config : $CONFIG"
echo "  started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"

TMPCONFIGS=()
PIDS=()
i=1
while IFS=' ' read -r chunk_start chunk_end; do
    TMPCFG=$(mktemp /tmp/batch_config_chunk_XXXX.json)
    TMPCONFIGS+=("$TMPCFG")
    TMPCFG_WIN="$(to_win_path "$TMPCFG")"
    "$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
c['download_start_date'] = '$chunk_start'
c['download_end_date'] = '$chunk_end'
with open(r'$TMPCFG_WIN', 'w') as f:
    json.dump(c, f, indent=2)
"
    LOG="/tmp/scan_chunk_${i}.log"
    echo "  Chunk $i: $chunk_start → $chunk_end  (log: $LOG)"
    bash "$SCRIPT_DIR/pipeline.sh" "$TMPCFG" --scan-only > "$LOG" 2>&1 &
    PIDS+=($!)
    i=$(( i + 1 ))
done <<< "$CHUNKS"

echo
echo "  All $N_CHUNKS chunks running (PIDs: ${PIDS[*]})"
echo "  Waiting for completion..."
echo

for pid in "${PIDS[@]}"; do
    wait "$pid" || true
done

# Clean up temp configs
for f in "${TMPCONFIGS[@]}"; do rm -f "$f"; done

# Count markers written
MARKER_COUNT=$(find "$SCRIPT_DIR/.pipeline_done" -type f 2>/dev/null | wc -l)

echo "========================================================"
echo "  Scan complete"
echo "  Markers written: $MARKER_COUNT"
echo "  Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Run 'bash pipeline.sh' to process days with data."
echo "========================================================"
