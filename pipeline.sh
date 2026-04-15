#!/usr/bin/env bash
#
# pipeline.sh — Download -> Organize -> Analyze  (one day at a time)
#
# Loops day-by-day from download_start_date to download_end_date.
# Each iteration: download that day's ZIPs, organize into sortie dirs,
# analyze each sortie, delete ZIPs.  Only one day of ZIPs on disk at once.
#
# Usage:
#   bash pipeline.sh                        # uses batch_config.json
#   bash pipeline.sh batch_config.json      # explicit config
#   bash pipeline.sh --dry-run              # preview without executing
#
set -euo pipefail

# ── Ctrl+C guard ───────────────────────────────────────────────────────────────
trap 'echo; read -p "  Abort pipeline? [y/N] " _yn; [[ "$_yn" =~ ^[Yy]$ ]] && exit 1 || echo "  Continuing..."' INT

# ── Args ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        *.json)    CONFIG="$(realpath "$arg")" ;;
    esac
done

# ── Path helpers (Git Bash /c/... -> C:/... for Windows Python) ───────────────
to_win_path() {
    if command -v cygpath &>/dev/null; then
        cygpath -w "$1"
    else
        echo "$1" | sed 's|^/\([a-zA-Z]\)/|\1:/|'
    fi
}

# ── Detect Python ─────────────────────────────────────────────────────────────
if command -v python3 &>/dev/null; then
    PYTHON=python3
elif command -v python &>/dev/null; then
    PYTHON=python
else
    echo "ERROR: Python not found on PATH."
    exit 1
fi

# ── Read config ────────────────────────────────────────────────────────────────
CONFIG_WIN="$(to_win_path "$CONFIG")"

cfg() {
    "$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
val = c.get('$1', '$2')
print(str(val) if val is not None else '$2')
"
}

DATA_ROOT="$(cfg data_root ".")"
[[ "$DATA_ROOT" != /* ]] && DATA_ROOT="$SCRIPT_DIR/$DATA_ROOT"

DOWNLOAD_SCRIPT="$(cfg download_script "~/Documents/GitHub/iads-export/scripts/iads_export_manual_multiple_download.sh")"
DOWNLOAD_SCRIPT="${DOWNLOAD_SCRIPT/#\~/$HOME}"

START_DATE="$(cfg download_start_date "$(date +%Y-%m-%d)")"
END_DATE="$(cfg download_end_date "$(date +%Y-%m-%d)")"
PATTERN="$(cfg download_pattern "*")"

SCRIPT_DIR_WIN="$(to_win_path "$SCRIPT_DIR")"
ANALYZE_CMD=("$PYTHON" "$SCRIPT_DIR_WIN/run_batch.py" "$CONFIG_WIN" --organize)
[[ $DRY_RUN -eq 1 ]] && ANALYZE_CMD+=(--dry-run)

# ── Generate day list ──────────────────────────────────────────────────────────
DAYS=$("$PYTHON" -c "
from datetime import date, timedelta
start = date.fromisoformat('$START_DATE')
end   = date.fromisoformat('$END_DATE')
d = start
while d <= end:
    print(d.isoformat())
    d += timedelta(days=1)
")
N_DAYS=$(echo "$DAYS" | wc -l)

# ── Timing helpers ─────────────────────────────────────────────────────────────
PIPELINE_START=$SECONDS

fmt_elapsed() {
    local secs=$1
    local h=$(( secs / 3600 ))
    local m=$(( (secs % 3600) / 60 ))
    local s=$(( secs % 60 ))
    if   [[ $h -gt 0 ]]; then printf "%dh %02dm %02ds" $h $m $s
    elif [[ $m -gt 0 ]]; then printf "%dm %02ds" $m $s
    else                      printf "%ds" $s
    fi
}

# ── Header ─────────────────────────────────────────────────────────────────────
echo "========================================================"
echo "  IADS Pipeline  (day-by-day)"
echo "  config      : $CONFIG"
echo "  data_root   : $DATA_ROOT"
echo "  dates       : $START_DATE  ->  $END_DATE  ($N_DAYS day(s))"
echo "  pattern     : $PATTERN"
echo "  started     : $(date '+%Y-%m-%d %H:%M:%S')"
[[ $DRY_RUN -eq 1 ]] && echo "  *** DRY RUN ***"
echo "========================================================"
echo

if [[ ! -f "$DOWNLOAD_SCRIPT" ]]; then
    echo "ERROR: download script not found: $DOWNLOAD_SCRIPT"
    echo "  Update 'download_script' in $CONFIG"
    exit 1
fi

mkdir -p "$DATA_ROOT"

# ── Per-day loop ───────────────────────────────────────────────────────────────
DAY_NUM=0
TOTAL_DL_S=0
TOTAL_AN_S=0

for DAY in $DAYS; do
    DAY_NUM=$(( DAY_NUM + 1 ))
    DAY_START=$SECONDS
    echo "════════════════════════════════════════════════════════"
    echo "  Day $DAY_NUM / $N_DAYS  —  $DAY"
    echo "════════════════════════════════════════════════════════"

    # ── Check for existing ZIPs ──────────────────────────────────────────────
    FLAT_ZIPS=$(  find "$DATA_ROOT" -maxdepth 1 -name "*.zip" 2>/dev/null | wc -l )
    SORTIE_ZIPS=$(find "$DATA_ROOT" -mindepth 2 -maxdepth 2 -name "*.zip" 2>/dev/null | wc -l )
    EXISTING_ZIPS=$(( FLAT_ZIPS + SORTIE_ZIPS ))

    # ── Download this day ────────────────────────────────────────────────────
    echo
    DL_START=$SECONDS

    if [[ $EXISTING_ZIPS -gt 0 ]]; then
        echo "  [1/2] Skipping download — $EXISTING_ZIPS ZIP(s) already on disk"
    elif [[ $DRY_RUN -eq 1 ]]; then
        echo "  [1/2] Downloading $DAY... [dry-run]"
        echo "  [dry-run] would download: $DAY (pattern '$PATTERN') -> $DATA_ROOT"
    else
        echo "  [1/2] Downloading $DAY..."
        pushd "$DATA_ROOT" > /dev/null
        TMPSCRIPT="$(mktemp /tmp/iads_dl_XXXX.sh)"
        trap 'rm -f "$TMPSCRIPT"' EXIT
        sed \
            -e "s|^START_DATE=.*|START_DATE=\"$DAY\"|" \
            -e "s|^END_DATE=.*|END_DATE=\"$DAY\"|" \
            -e "s|^FILENAME_PATTERN=.*|FILENAME_PATTERN=\"$PATTERN\"|" \
            "$DOWNLOAD_SCRIPT" > "$TMPSCRIPT"
        chmod +x "$TMPSCRIPT"
        echo "Y" | bash "$TMPSCRIPT"
        popd > /dev/null
    fi

    DL_ELAPSED=$(( SECONDS - DL_START ))
    TOTAL_DL_S=$(( TOTAL_DL_S + DL_ELAPSED ))
    echo "  Download: $(fmt_elapsed $DL_ELAPSED)"

    # ── Organize + Analyze ───────────────────────────────────────────────────
    echo
    echo "  [2/2] Organize + Analyze..."
    AN_START=$SECONDS
    "${ANALYZE_CMD[@]}"
    AN_ELAPSED=$(( SECONDS - AN_START ))
    TOTAL_AN_S=$(( TOTAL_AN_S + AN_ELAPSED ))

    DAY_ELAPSED=$(( SECONDS - DAY_START ))
    echo "  Analyze : $(fmt_elapsed $AN_ELAPSED)"
    echo "  Day total: $(fmt_elapsed $DAY_ELAPSED)"
    echo
done

TOTAL_ELAPSED=$(( SECONDS - PIPELINE_START ))
echo "========================================================"
echo "  Pipeline complete"
echo "  Days processed : $N_DAYS"
echo "  Download time  : $(fmt_elapsed $TOTAL_DL_S)"
echo "  Analysis time  : $(fmt_elapsed $TOTAL_AN_S)"
echo "  Total elapsed  : $(fmt_elapsed $TOTAL_ELAPSED)"
echo "  Finished       : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
