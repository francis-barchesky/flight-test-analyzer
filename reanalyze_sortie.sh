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

    # Organize flat ZIPs then immediately analyze — combined so the batch triggered
    # by --organize doesn't delete the freshly-moved ZIPs before --sortie sees them
    echo
    echo "  [2/2] Organizing & analyzing..."
    "$PYTHON" "$SCRIPT_DIR/run_batch.py" "$TMP_CFG" --organize --sortie "$SORTIE"

    rm -f "$TMP_CFG"
    trap - EXIT
    echo
done

echo "========================================================"
echo "  reanalyze_sortie complete"
echo "  Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"
