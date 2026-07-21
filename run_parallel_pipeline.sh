#!/usr/bin/env bash
#
# run_parallel_pipeline.sh — Split the pipeline date range into N chunks and
# run each in parallel, each with its own data_root to avoid staging collisions.
#
# Usage:
#   bash run_parallel_pipeline.sh              # 8 chunks, 3 workers each
#   bash run_parallel_pipeline.sh --dry-run
#   bash run_parallel_pipeline.sh --chunks 4 --workers 5
#
# After completion:
#   bash collect_results.sh
#
set -euo pipefail

unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
PYTHON="$PY313"

N_CHUNKS=8
WORKERS=3
DRY_RUN_FLAG=""

# ── Parse args ────────────────────────────────────────────────────────────────
i=1
while [[ $i -le $# ]]; do
    arg="${!i}"
    case "$arg" in
        --dry-run) DRY_RUN_FLAG="--dry-run" ;;
        --chunks)
            i=$((i + 1)); N_CHUNKS="${!i}" ;;
        --workers)
            i=$((i + 1)); WORKERS="${!i}" ;;
    esac
    i=$((i + 1))
done

echo "========================================================"
echo "  Parallel Pipeline Launcher"
echo "  chunks  : $N_CHUNKS"
echo "  workers : $WORKERS per chunk  (${N_CHUNKS}×${WORKERS} = $((N_CHUNKS * WORKERS)) total)"
echo "  started : $(date '+%Y-%m-%d %H:%M:%S')"
[[ -n "$DRY_RUN_FLAG" ]] && echo "  *** DRY RUN ***"
echo "========================================================"
echo

# ── Generate chunk configs ────────────────────────────────────────────────────
echo "Generating $N_CHUNKS chunk configs..."
CONFIG_PATHS=$("$PYTHON" "$SCRIPT_DIR/setup_pipeline_chunks.py" \
    "$SCRIPT_DIR/batch_config.json" \
    --chunks  "$N_CHUNKS" \
    --workers "$WORKERS"  \
    --out-dir "$SCRIPT_DIR/pipeline_chunks")
echo

# ── Launch all chunks in background ──────────────────────────────────────────
PIDS=()
CHUNK_NUM=0
LOGS_DIR="$SCRIPT_DIR/pipeline_chunks"

while IFS= read -r cfg_path; do
    [[ -z "$cfg_path" ]] && continue
    CHUNK_NUM=$((CHUNK_NUM + 1))
    LOG="$LOGS_DIR/chunk_${CHUNK_NUM}.log"
    echo "  Launching chunk $CHUNK_NUM — log: pipeline_chunks/chunk_${CHUNK_NUM}.log"
    bash "$SCRIPT_DIR/pipeline.sh" $DRY_RUN_FLAG "$cfg_path" > "$LOG" 2>&1 &
    PIDS+=($!)
done <<< "$CONFIG_PATHS"

echo
echo "All $CHUNK_NUM chunks launched (PIDs: ${PIDS[*]})"
echo "Monitor progress with:  tail -f pipeline_chunks/chunk_N.log"
echo

# ── Wait for all chunks ───────────────────────────────────────────────────────
ALL_OK=1
for idx in "${!PIDS[@]}"; do
    pid="${PIDS[$idx]}"
    chunk=$((idx + 1))
    if wait "$pid"; then
        echo "  Chunk $chunk  OK"
    else
        echo "  Chunk $chunk  FAILED  (PID $pid — check pipeline_chunks/chunk_${chunk}.log)"
        ALL_OK=0
    fi
done

echo
echo "========================================================"
if [[ $ALL_OK -eq 1 ]]; then
    echo "  All chunks complete."
    echo "  Run:  bash collect_results.sh"
else
    echo "  Some chunks failed — review logs before collecting."
fi
echo "  Finished : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
