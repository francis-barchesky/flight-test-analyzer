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
#   bash pipeline.sh --reset-markers        # drop .pipeline_done markers in the
#                                           # configured date range before running,
#                                           # forcing a fresh re-evaluation
#
set -euo pipefail

# ── Clear inherited PYTHONHOME/PYTHONPATH ──────────────────────────────────────
# Mixing PYTHONHOME/PYTHONPATH across CPython versions causes
# "AssertionError: SRE module mismatch" when stdlib resolution finds a
# different version's _sre. Always launch Python with a clean env.
unset PYTHONHOME PYTHONPATH

# ── Ctrl+C guard ───────────────────────────────────────────────────────────────
trap 'echo; read -p "  Abort pipeline? [y/N] " _yn; [[ "$_yn" =~ ^[Yy]$ ]] && exit 1 || echo "  Continuing..."' INT

# ── Args ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
DRY_RUN=0
RESET_MARKERS=0

for arg in "$@"; do
    case "$arg" in
        --dry-run)       DRY_RUN=1 ;;
        --reset-markers) RESET_MARKERS=1 ;;
        *.json)          CONFIG="$(realpath "$arg")" ;;
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

# Detect the Windows C: drive mount point for this shell environment.
# Git Bash uses /c/, WSL uses /mnt/c/.
if [[ -d "/mnt/c/Users" ]]; then
    _WIN_C="/mnt/c"
else
    _WIN_C="/c"
fi

# ── Detect Python ─────────────────────────────────────────────────────────────
# Prefer the known Python 3.13.3 install (the project needs 3.13; 3.12 on PATH
# in Git Bash has triggered SRE mismatches). Respect a pre-set $PYTHON override.
PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
if [[ -n "${PYTHON:-}" ]] && command -v "$PYTHON" &>/dev/null; then
    :  # already set
elif [[ -x "$PY313" ]]; then
    PYTHON="$PY313"
elif command -v py &>/dev/null; then
    PYTHON="py -3.13"
elif command -v python3 &>/dev/null; then
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
import json, datetime
c = json.load(open(r'$CONFIG_WIN'))
val = c.get('$1', '$2')
if val is None:
    val = '$2'
if isinstance(val, list):
    # Join multiple regex patterns into an alternation group, e.g.
    # ['AFCS_del', 'AFCS_MiscAnalysis'] -> '(AFCS_del|AFCS_MiscAnalysis)'
    val = '(' + '|'.join(str(x) for x in val) + ')'
val = str(val)
if val.lower() == 'today':
    val = datetime.date.today().isoformat()
print(val)
"
}

DATA_ROOT="$(cfg data_root ".")"
[[ "$DATA_ROOT" != /* ]] && DATA_ROOT="$SCRIPT_DIR/$DATA_ROOT"
DATA_ROOT="$(cd "$DATA_ROOT" && pwd)"

DOWNLOAD_SCRIPT="$(cfg download_script "~/Documents/GitHub/iads-export/scripts/iads_export_manual_multiple_download.sh")"
DOWNLOAD_SCRIPT="${DOWNLOAD_SCRIPT/#\~/$HOME}"
# Normalise Windows drive prefix to match the current shell's mount convention.
DOWNLOAD_SCRIPT="$(echo "$DOWNLOAD_SCRIPT" | sed "s|^/mnt/c/|${_WIN_C}/|; s|^/c/|${_WIN_C}/|")"

START_DATE="$(cfg download_start_date "$(date +%Y-%m-%d)")"
END_DATE="$(cfg download_end_date "$(date +%Y-%m-%d)")"
PATTERN="$(cfg download_pattern "*")"
MIN_FREE_GB="$(cfg min_free_gb "10")"

SCRIPT_DIR_WIN="$(to_win_path "$SCRIPT_DIR")"
ANALYZE_CMD=("$PYTHON" "$SCRIPT_DIR_WIN/run_batch.py" "$CONFIG_WIN" --organize --zips-only)
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
" | tr -d '\r')
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

# ── Disk space guard ───────────────────────────────────────────────────────────
check_disk_space() {
    local required_gb=$1
    local path=$2
    local free_kb
    free_kb=$(df -k "$path" 2>/dev/null | awk 'NR==2 {print $4}')
    if [[ -z "$free_kb" ]]; then
        echo "  [!] WARNING: could not check free disk space at $path — proceeding anyway"
        return 0
    fi
    local free_gb
    free_gb=$(awk "BEGIN {printf \"%.1f\", $free_kb / 1048576}")
    if awk "BEGIN {exit !($free_gb < $required_gb)}"; then
        echo "  ERROR: insufficient disk space — ${free_gb} GB free, ${required_gb} GB required."
        echo "         Free up space or lower --chunks to reduce concurrent downloads, then re-run."
        exit 1
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

# ── AWS SSO check ──────────────────────────────────────────────────────────────
if [[ $DRY_RUN -eq 0 ]]; then
    echo "  Checking AWS SSO login..."
    if ! aws sts get-caller-identity &>/dev/null; then
        echo
        echo "ERROR: AWS SSO session is not active or has expired."
        echo "  Run:  aws sso login"
        echo "  Then re-run this pipeline."
        exit 1
    fi
    echo "  AWS SSO OK"
    echo
fi

mkdir -p "$DATA_ROOT"

# ── Optional: drop markers in the current window ──────────────────────────────
# Use when you have markers from prior runs that you don't trust (e.g. they were
# written while stranded flat ZIPs were blocking downloads). Only affects days
# inside [START_DATE, END_DATE] so other ranges' markers are preserved.
if (( RESET_MARKERS == 1 )); then
    reset_count=0
    for DAY in $DAYS; do
        if [[ -f "$DATA_ROOT/.pipeline_done/$DAY" ]]; then
            rm "$DATA_ROOT/.pipeline_done/$DAY"
            reset_count=$(( reset_count + 1 ))
        fi
    done
    echo "  --reset-markers: cleared $reset_count marker(s) in [$START_DATE..$END_DATE]"
    echo
fi

# ── Per-day loop ───────────────────────────────────────────────────────────────
DAY_NUM=0
TOTAL_DL_S=0
TOTAL_AN_S=0
TOTAL_TRULY_DONE=0
TOTAL_STRANDED_DAYS=0

for DAY in $DAYS; do
    DAY_NUM=$(( DAY_NUM + 1 ))
    DAY_START=$SECONDS
    echo "════════════════════════════════════════════════════════"
    echo "  Day $DAY_NUM / $N_DAYS  —  $DAY"
    echo "════════════════════════════════════════════════════════"

    # ── Check for existing ZIPs / done marker ───────────────────────────────
    # Only flat (unorganized) ZIPs at data_root level count as a pending download.
    # ZIPs already in sortie subdirs are from previous runs and should not block re-download.
    # A .pipeline_done/YYYY-MM-DD marker file means this day was already downloaded+organized.
    DONE_MARKER="$DATA_ROOT/.pipeline_done/$DAY"
    FLAT_ZIPS=$(find "$DATA_ROOT" -maxdepth 1 -name "*.zip" 2>/dev/null | wc -l)

    # ── Skip day entirely if already processed and no pending ZIPs ──────────
    if [[ -f "$DONE_MARKER" && $FLAT_ZIPS -eq 0 ]]; then
        echo "  Skipping — already processed ($DONE_MARKER)"
        continue
    fi

    # ── Download this day ────────────────────────────────────────────────────
    echo
    DL_START=$SECONDS

    if [[ -f "$DONE_MARKER" ]]; then
        echo "  [1/2] Skipping download — already processed ($DONE_MARKER)"
    elif [[ $FLAT_ZIPS -gt 0 ]]; then
        echo "  [1/2] Skipping download — $FLAT_ZIPS flat ZIP(s) already on disk"
    elif [[ $DRY_RUN -eq 1 ]]; then
        echo "  [1/2] Downloading $DAY... [dry-run]"
        echo "  [dry-run] would download: $DAY (pattern '$PATTERN') -> $DATA_ROOT"
    else
        echo "  [1/2] Downloading $DAY..."
        check_disk_space "$MIN_FREE_GB" "$DATA_ROOT"
        pushd "$DATA_ROOT" > /dev/null
        TMPSCRIPT="$(mktemp /tmp/iads_dl_XXXX.sh)"
        trap 'rm -f "$TMPSCRIPT"' EXIT
        # Use '#' as sed delimiter so a regex '|' alternation inside $PATTERN
        # (e.g. '(AFCS_del|AFCS_MiscAnalysis)') doesn't terminate the s-command.
        sed \
            -e "s#^START_DATE=.*#START_DATE=\"$DAY\"#" \
            -e "s#^END_DATE=.*#END_DATE=\"$DAY\"#" \
            -e "s#^FILENAME_PATTERN=.*#FILENAME_PATTERN=\"$PATTERN\"#" \
            "$DOWNLOAD_SCRIPT" > "$TMPSCRIPT"
        chmod +x "$TMPSCRIPT"
        bash "$TMPSCRIPT" <<< "Y"
        popd > /dev/null
        # Marker is written below only if files actually landed — empty-download
        # days should remain unmarked so future re-runs re-attempt them in case
        # data lands on S3 later.
    fi

    DL_ELAPSED=$(( SECONDS - DL_START ))
    TOTAL_DL_S=$(( TOTAL_DL_S + DL_ELAPSED ))
    echo "  Download: $(fmt_elapsed $DL_ELAPSED)"

    # Count flat ZIPs AFTER download but BEFORE analyze --organize, since
    # --organize moves them into sortie subdirs and would zero the count.
    POST_DL_FLAT=$(find "$DATA_ROOT" -maxdepth 1 -name "*.zip" 2>/dev/null | wc -l)

    # ── Organize + Analyze ───────────────────────────────────────────────────
    echo
    echo "  [2/2] Organize + Analyze..."
    AN_START=$SECONDS
    "${ANALYZE_CMD[@]}" || echo "  [!] batch exited non-zero (some sorties errored — pipeline continues)"
    AN_ELAPSED=$(( SECONDS - AN_START ))
    TOTAL_AN_S=$(( TOTAL_AN_S + AN_ELAPSED ))

    # Did organize+analyze actually do anything useful? "Truly done" means at
    # least one flat ZIP got moved into a sortie dir — if flat-ZIP count is
    # unchanged after analyze, the ZIPs are stranded (e.g. malformed filenames
    # that organize can't parse a sortie tag from) and writing a marker would
    # falsely permanent-skip the day forever.
    POST_ANAL_FLAT=$(find "$DATA_ROOT" -maxdepth 1 -name "*.zip" 2>/dev/null | wc -l)
    HAD_ZIPS=0
    (( FLAT_ZIPS > 0 || POST_DL_FLAT > 0 )) && HAD_ZIPS=1
    ZIPS_ORGANIZED=$(( POST_DL_FLAT - POST_ANAL_FLAT ))
    TRULY_DONE=0
    if (( HAD_ZIPS == 1 )) && (( ZIPS_ORGANIZED > 0 )); then
        TRULY_DONE=1
    fi

    if [[ $DRY_RUN -eq 0 ]]; then
        if (( TRULY_DONE == 1 )); then
            mkdir -p "$DATA_ROOT/.pipeline_done"
            touch "$DONE_MARKER"
            TOTAL_TRULY_DONE=$(( TOTAL_TRULY_DONE + 1 ))
        else
            # Not truly done. If a marker was carried over from a prior
            # (presumably bogus) run, remove it so this day stays eligible
            # for retry on the next pipeline invocation.
            if [[ -f "$DONE_MARKER" ]]; then
                rm "$DONE_MARKER"
                echo "  [!] removed stale marker for $DAY (no work completed this iteration)"
            fi
            if (( HAD_ZIPS == 1 )) && (( POST_ANAL_FLAT > 0 )); then
                echo "  [!] $POST_ANAL_FLAT flat ZIP(s) stranded at data_root — organize couldn't parse a sortie tag:"
                find "$DATA_ROOT" -maxdepth 1 -name '*.zip' -printf '      %f\n' 2>/dev/null
                TOTAL_STRANDED_DAYS=$(( TOTAL_STRANDED_DAYS + 1 ))
            fi
        fi
    fi

    DAY_ELAPSED=$(( SECONDS - DAY_START ))
    echo "  Analyze : $(fmt_elapsed $AN_ELAPSED)"
    echo "  Day total: $(fmt_elapsed $DAY_ELAPSED)"
    echo
done

TOTAL_ELAPSED=$(( SECONDS - PIPELINE_START ))
echo "========================================================"
echo "  Pipeline complete"
echo "  Days processed   : $N_DAYS"
echo "  Days truly done  : $TOTAL_TRULY_DONE"
echo "  Days stranded    : $TOTAL_STRANDED_DAYS"
echo "  Download time    : $(fmt_elapsed $TOTAL_DL_S)"
echo "  Analysis time    : $(fmt_elapsed $TOTAL_AN_S)"
echo "  Total elapsed    : $(fmt_elapsed $TOTAL_ELAPSED)"
echo "  Finished         : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
