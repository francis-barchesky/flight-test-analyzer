#!/usr/bin/env bash
# reanalyze_sortie.sh — Re-download and re-analyze one or more named sorties.
#
# Usage:
#   bash reanalyze_sortie.sh SORTIE1 [SORTIE2 ...]
#   bash reanalyze_sortie.sh my_config.json SORTIE1 [SORTIE2 ...]
#
# Examples:
#   bash reanalyze_sortie.sh S115_1_N208B S115_2_N208B
#   bash reanalyze_sortie.sh G004_ZKMLN
set -euo pipefail
unset PYTHONHOME PYTHONPATH

# Minimum free disk space that must remain after the download completes.
# The script aborts before downloading if this floor would be breached.
MIN_FREE_GB=5

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
SORTIES=()

for arg in "$@"; do
    case "$arg" in
        *.json) CONFIG="$(realpath "$arg")" ;;
        *)      SORTIES+=("$arg") ;;
    esac
done

if [[ ${#SORTIES[@]} -eq 0 ]]; then
    echo "Usage: bash reanalyze_sortie.sh [config.json] SORTIE1 [SORTIE2 ...]"
    exit 1
fi

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
SCRIPT_DIR_WIN="$(to_win_path "$SCRIPT_DIR")"

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
echo

# ── Disk space pre-flight check ───────────────────────────────────────────────
# Build IADS-style tags for all requested sorties so we can grep the S3 listing.
SORTIE_TAGS=()
for _S in "${SORTIES[@]}"; do
    _TAG=$("$PYTHON" -c "
parts = '$_S'.split('_')
if len(parts) == 3:
    prefix, leg, tail = parts
    print(f'{prefix}{tail}_{leg}')
else:
    prefix, tail = parts[0], parts[-1]
    print(f'{prefix}{tail}')
" | tr -d '\r')
    SORTIE_TAGS+=("$_TAG")
done

# Sum bytes for all files in the S3 listings that match any of our sortie tags.
# `aws s3 ls --recursive` format: "date time size key"
TOTAL_BYTES=0
for _TAG in "${SORTIE_TAGS[@]}"; do
    _BYTES=$(grep -h "$_TAG" "$EXPORTS_CACHE" "$ANALYSIS_CACHE" 2>/dev/null \
             | awk '{sum += $3} END {print sum+0}')
    TOTAL_BYTES=$(( TOTAL_BYTES + _BYTES ))
done
TOTAL_MB=$(( TOTAL_BYTES / 1048576 ))
TOTAL_GB_FRAC=$(awk "BEGIN {printf \"%.1f\", $TOTAL_BYTES/1073741824}")

# Available bytes on the data_root filesystem (use df on the script dir as proxy).
AVAIL_BYTES=$(df -B1 "$SCRIPT_DIR" 2>/dev/null | awk 'NR==2{print $4}')
AVAIL_MB=$(( ${AVAIL_BYTES:-0} / 1048576 ))
AVAIL_GB_FRAC=$(awk "BEGIN {printf \"%.1f\", ${AVAIL_BYTES:-0}/1073741824}")

# Require 10 % headroom above the estimated download size.
REQUIRED_BYTES=$(awk "BEGIN {printf \"%d\", $TOTAL_BYTES * 1.10}")

MIN_FREE_BYTES=$(awk "BEGIN {printf \"%d\", $MIN_FREE_GB * 1073741824}")

echo "  Disk space check:"
echo "    estimated download : ${TOTAL_GB_FRAC} GB  (${TOTAL_MB} MB across ${#SORTIES[@]} sortie(s))"
echo "    available on disk  : ${AVAIL_GB_FRAC} GB  (${AVAIL_MB} MB)"
echo "    minimum free after : ${MIN_FREE_GB} GB  (hardcoded protection)"

FAILED=0

# Check 1: enough room for the download + 10% headroom
if [[ "${AVAIL_BYTES:-0}" -lt "${REQUIRED_BYTES}" && "${TOTAL_BYTES}" -gt 0 ]]; then
    NEEDED_GB=$(awk "BEGIN {printf \"%.1f\", $REQUIRED_BYTES/1073741824}")
    echo "  ERROR: not enough space for download. Need ~${NEEDED_GB} GB (10% headroom), only ${AVAIL_GB_FRAC} GB available."
    FAILED=1
fi

# Check 2: after download, at least MIN_FREE_GB must remain free
POST_AVAIL_BYTES=$(( ${AVAIL_BYTES:-0} - TOTAL_BYTES ))
if [[ $POST_AVAIL_BYTES -lt $MIN_FREE_BYTES && $MIN_FREE_GB -gt 0 ]]; then
    POST_GB=$(awk "BEGIN {printf \"%.1f\", $POST_AVAIL_BYTES/1073741824}")
    echo "  ERROR: post-download free space would be ${POST_GB} GB, below --min-free-gb ${MIN_FREE_GB} GB floor."
    FAILED=1
fi

if [[ $FAILED -eq 1 ]]; then
    exit 1
fi
echo "    check passed"
echo

# ── Read download_script from config ──────────────────────────────────────────
DOWNLOAD_SCRIPT=$("$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
print(c['download_script'])
" | tr -d '\r')

if [[ ! -f "$DOWNLOAD_SCRIPT" ]]; then
    echo "ERROR: download_script not found: $DOWNLOAD_SCRIPT"
    exit 1
fi

# ── Process each sortie ────────────────────────────────────────────────────────
ALL_SORTIES_STR="${SORTIES[*]}"
echo "========================================================"
echo "  reanalyze_sortie"
echo "  config  : $CONFIG"
echo "  sorties : $ALL_SORTIES_STR"
echo "  started : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
echo

for SORTIE in "${SORTIES[@]}"; do
    echo "────────────────────────────────────────────────────────"
    echo "  Sortie: $SORTIE"
    echo "────────────────────────────────────────────────────────"

    # Resolve data_root for this tail from data_root_map
    DATA_ROOT=$("$PYTHON" -c "
import json, os
c = json.load(open(r'$CONFIG_WIN'))
tail = '$SORTIE'.rsplit('_', 1)[-1]
dr = c.get('data_root_map', {}).get(tail, c.get('data_root', '.'))
script_dir = r'$SCRIPT_DIR_WIN'
if not os.path.isabs(dr):
    dr = os.path.normpath(os.path.join(script_dir, dr))
print(dr)
" | tr -d '\r')
    mkdir -p "$DATA_ROOT"
    DATA_ROOT="$(cd "$DATA_ROOT" && pwd)"
    DATA_ROOT_WIN="$(to_win_path "$DATA_ROOT")"

    # Build sortie tag (S115_1_N208B -> S115N208B_1, S115_N208B -> S115N208B)
    SORTIE_TAG=$("$PYTHON" -c "
parts = '$SORTIE'.split('_')
if len(parts) == 3:
    prefix, leg, tail = parts
    print(f'{prefix}{tail}_{leg}')
else:
    prefix, tail = parts[0], parts[-1]
    print(f'{prefix}{tail}')
" | tr -d '\r')

    # Build download pattern from config's download_pattern list
    FILENAME_PATTERN=$("$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
patterns = c.get('download_pattern', [])
if isinstance(patterns, str):
    patterns = [patterns]
combined = '(' + '|'.join(patterns) + ').*$SORTIE_TAG'
print(combined)
" | tr -d '\r')

    echo "  data_root : $DATA_ROOT"
    echo "  tag       : $SORTIE_TAG"
    echo "  pattern   : $FILENAME_PATTERN"
    echo

    # Download ZIPs to DATA_ROOT
    echo "  [1/2] Downloading..."
    pushd "$DATA_ROOT" > /dev/null
    TMP_DL=$(mktemp /tmp/iads_dl_XXXX.sh)
    sed -e 's#^START_DATE=.*#START_DATE="2025-01-01"#' \
        -e "s#^END_DATE=.*#END_DATE=\"$(date +%Y-%m-%d)\"#" \
        -e "s#^FILENAME_PATTERN=.*#FILENAME_PATTERN=\"$FILENAME_PATTERN\"#" \
        "$DOWNLOAD_SCRIPT" > "$TMP_DL"
    chmod +x "$TMP_DL"
    IADS_EXPORTS_LISTING="$EXPORTS_CACHE" \
    IADS_ANALYSIS_LISTING="$ANALYSIS_CACHE" \
        echo Y | bash "$TMP_DL"
    rm -f "$TMP_DL"
    popd > /dev/null

    # Write temp config adjacent to run_batch.py so data_root_map relative paths
    # resolve correctly (run_batch.py --sortie resolves them relative to config_path)
    TMP_CFG="$SCRIPT_DIR/.reanalyze_$$.json"
    TMP_CFG_WIN="$(to_win_path "$TMP_CFG")"
    trap 'rm -f "$TMP_CFG"' EXIT
    "$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
c['data_root'] = r'$DATA_ROOT_WIN'
with open(r'$TMP_CFG_WIN', 'w') as f:
    json.dump(c, f, indent=2)
"

    # Organize flat ZIPs under a shared lock so parallel instances don't race
    # when moving newly-downloaded ZIPs into their per-sortie subdirectories.
    # The lock covers only the organize step; analysis runs without it so
    # multiple sorties can analyze concurrently.
    ORGANIZE_LOCK="/tmp/iads_organize.lock"
    echo
    echo "  [2/2] Organizing & analyzing..."
    flock -x "$ORGANIZE_LOCK" \
        "$PYTHON" "$SCRIPT_DIR/run_batch.py" "$TMP_CFG" --organize
    "$PYTHON" "$SCRIPT_DIR/run_batch.py" "$TMP_CFG" --sortie "$SORTIE"

    rm -f "$TMP_CFG"
    trap - EXIT
    echo
done

echo "========================================================"
echo "  reanalyze_sortie complete"
echo "  Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
