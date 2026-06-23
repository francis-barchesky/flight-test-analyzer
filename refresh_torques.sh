#!/usr/bin/env bash
#
# refresh_torques.sh — pull fresh iads_servo_torques_datagroup_* ZIPs from S3
# for every sortie dir and merge them into each sortie's existing analysis JSON.
#
# Why this exists:
#   The previous iads_servo_torques_datagroup exports were all-NaN due to a
#   period mismatch in the DataGroup CSV. After regenerating the exports with
#   the corrected CSV, this script pulls each sortie's torque ZIP and re-runs
#   analyze, which (via the merge patch in analyze_iads.py:_write_or_merge_result)
#   augments the existing JSON's torque_stats with the new A429 columns without
#   touching episodes / mode_transitions / flight_plots.
#
# Disk footprint: one sortie's torque ZIP at a time (typically <100 KB).
#
# Usage:
#   bash refresh_torques.sh                              # all sortie dirs
#   bash refresh_torques.sh --dry-run                    # preview
#   bash refresh_torques.sh --only S119_2_N208B,S125_N208B
#
set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="$SCRIPT_DIR"

ANALYSIS_BUCKET="s3://merlin-pilot-iads-analysis"
DRY_RUN=0
ONLY_LIST=""

i=0
for arg in "$@"; do
    i=$((i+1))
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --only)    next=$((i+1)); ONLY_LIST="${!next:-}" ;;
        --only=*)  ONLY_LIST="${arg#--only=}" ;;
    esac
done

# Detect Python
PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
if [[ -x "$PY313" ]]; then
    PYTHON="$PY313"
elif command -v py &>/dev/null; then
    PYTHON="py -3.13"
else
    PYTHON="python"
fi

# ── Cache analysis bucket listing once ────────────────────────────────────────
TMP=$(mktemp -d /tmp/refresh_torques.XXXX)
trap 'rm -rf "$TMP"' EXIT
ANALYSIS_LIST="$TMP/analysis.ls"

echo "Listing $ANALYSIS_BUCKET ..."
aws s3 ls "$ANALYSIS_BUCKET" --recursive > "$ANALYSIS_LIST"
echo "  $(wc -l < "$ANALYSIS_LIST") objects"
echo

# ── Map sortie dir name to S3 sortie tag (same logic as recover_stubs.sh) ─────
sortie_to_s3tag() {
    local dir="$1" head tail leg n_parts
    n_parts=$(echo "$dir" | awk -F_ '{print NF}')
    if [[ "$n_parts" -eq 2 ]]; then
        head="${dir%_*}"; tail="${dir##*_}"; leg=""
    elif [[ "$n_parts" -eq 3 ]]; then
        head="$(echo "$dir" | awk -F_ '{print $1}')"
        leg="$(echo "$dir" | awk -F_ '{print $2}')"
        tail="$(echo "$dir" | awk -F_ '{print $3}')"
    else
        echo ""; return
    fi
    if [[ -n "$leg" ]]; then echo "${head}${tail}_${leg}"; else echo "${head}${tail}"; fi
}

# ── Iterate sortie dirs ───────────────────────────────────────────────────────
SORTIE_DIRS=()
for dir in "$DATA_ROOT"/*/; do
    [[ -d "$dir" ]] || continue
    name="$(basename "$dir")"
    [[ "$name" == "."* ]] && continue
    [[ "$name" == "plots" ]] && continue
    [[ "$name" == "__pycache__" ]] && continue
    [[ "$name" != [SG]* ]] && continue
    if [[ -n "$ONLY_LIST" ]]; then
        [[ ",${ONLY_LIST}," == *",${name},"* ]] || continue
    fi
    SORTIE_DIRS+=("$name")
done

if [[ ${#SORTIE_DIRS[@]} -eq 0 ]]; then
    echo "No sortie dirs to process."
    exit 0
fi

echo "Sortie dirs to process: ${#SORTIE_DIRS[@]}"
echo

TOTAL_DL=0
TOTAL_OK=0
SKIPPED_NO_S3=()
SKIPPED_NO_JSON=()
ANALYZE_FAILED=()

idx=0
for dir_name in "${SORTIE_DIRS[@]}"; do
    idx=$((idx+1))
    s3tag=$(sortie_to_s3tag "$dir_name")
    if [[ -z "$s3tag" ]]; then
        echo "[$idx/${#SORTIE_DIRS[@]}] $dir_name  SKIP (can't map)"
        SKIPPED_NO_S3+=("$dir_name")
        continue
    fi
    dest_dir="$DATA_ROOT/$dir_name"

    # Skip if no existing analysis JSON to merge into (we don't want stubs from
    # torque-only analyses to overwrite — that defeats the purpose).
    existing_json=$(find "$dest_dir" -maxdepth 1 -name 'analysis_*.json' -not -name '*_hires*' 2>/dev/null | head -1)
    if [[ -z "$existing_json" ]]; then
        echo "[$idx/${#SORTIE_DIRS[@]}] $dir_name  SKIP (no existing analysis JSON)"
        SKIPPED_NO_JSON+=("$dir_name")
        continue
    fi

    # Find the SINGLE NEWEST torque ZIP for this sortie tag.
    #
    # Candidate filename shapes:
    #   iads_servo_torques_datagroup_<TAG>.zip                       canonical
    #   iads_servo_torques_datagroup_<TAG>_<14digit-timestamp>.zip   workflow re-run
    #   iads_servo_torques_datagroup_<TAG>_<alpha-suffix>.zip        e.g. _ACS
    #   iads_servo_torques_datagroup_<TAG>_<alpha>_<14digit>.zip     re-run of variant
    #
    # The all-NaN exports we ran into were the original (Jun 18) canonical
    # files; regenerated exports show up as timestamped versions. The S3
    # LastModified date in $1/$2 is the source of truth — sort lexically
    # descending and pick the top match per sortie.
    #
    # Excludes _test_points* (different content) and refuses legged sortie
    # variants ("_<digit>") — those belong to different sortie dirs.
    # After "_<TAG>_", a segment of 1-3 digits = sortie leg number (reject —
    # belongs to a different sortie dir). 14 digits = workflow timestamp (keep).
    # Alpha suffix = variant like _ACS (keep).
    TORQUE_KEYS=$(awk -v tag="$s3tag" '
        $4 ~ ("(^|/)iads_servo_torques_datagroup_" tag "([._A-Za-z0-9][^/]*)?\\.zip$") &&
        $4 !~ ("(^|/)iads_servo_torques_datagroup_" tag "_[0-9]{1,3}(_|\\.)") &&
        $4 !~ /_test_points/ &&
        $4 !~ /test_point_exports/ {
            print $1 " " $2 " " $4
        }' "$ANALYSIS_LIST" | sort -r | head -1 | awk '{print $3}')

    if [[ -z "$TORQUE_KEYS" ]]; then
        echo "[$idx/${#SORTIE_DIRS[@]}] $dir_name  SKIP (no torque ZIP in analysis bucket)"
        SKIPPED_NO_S3+=("$dir_name")
        continue
    fi

    echo "── [$idx/${#SORTIE_DIRS[@]}] $dir_name  (tag=$s3tag) ──"

    # Download
    n_this=0
    while IFS= read -r key; do
        [[ -z "$key" ]] && continue
        local_file="$dest_dir/$(basename "$key")"
        if [[ -f "$local_file" ]]; then
            echo "  already have $(basename "$key")"
            n_this=$((n_this+1))   # still counts as a ZIP to analyze
            continue
        fi
        if [[ $DRY_RUN -eq 1 ]]; then
            echo "  [dry-run] would: aws s3 cp $ANALYSIS_BUCKET/$key  $local_file"
            n_this=$((n_this+1))
        else
            echo "  download $ANALYSIS_BUCKET/$key"
            aws s3 cp "$ANALYSIS_BUCKET/$key" "$local_file" --only-show-errors
            TOTAL_DL=$((TOTAL_DL+1))
            n_this=$((n_this+1))
        fi
    done <<< "$TORQUE_KEYS"

    # Analyze (merge mode kicks in since torque-only ZIP -> 0 episodes,
    # existing JSON has episodes -> _write_or_merge_result merges torque_stats)
    # n_this counts both new downloads and "already have" hits — covers the
    # case where a prior interrupted run already pulled the ZIP locally.
    n_zips_in_dir=$(find "$dest_dir" -maxdepth 1 -name 'iads_servo_torques_datagroup_*.zip' 2>/dev/null | wc -l)
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "  [dry-run] would: $PYTHON run_batch.py --zips-only  (torque-only -> merge)"
    elif [[ $n_zips_in_dir -eq 0 ]]; then
        echo "  no torque ZIPs available — skipping analyze"
    else
        echo "  analyze (merge mode)..."
        if "$PYTHON" "$SCRIPT_DIR/run_batch.py" "$SCRIPT_DIR/batch_config.json" --zips-only; then
            TOTAL_OK=$((TOTAL_OK+1))
        else
            echo "  ANALYZE FAILED"
            ANALYZE_FAILED+=("$dir_name")
        fi
    fi
done

echo
echo "Refresh summary:"
echo "  Sorties processed (analyze ok):   $TOTAL_OK"
echo "  Files downloaded:                 $TOTAL_DL"
if [[ ${#SKIPPED_NO_JSON[@]} -gt 0 ]]; then
    echo "  Skipped (no existing analysis):   ${#SKIPPED_NO_JSON[@]}"
fi
if [[ ${#SKIPPED_NO_S3[@]} -gt 0 ]]; then
    echo "  Skipped (no torque ZIP in S3):    ${#SKIPPED_NO_S3[@]}"
    for s in "${SKIPPED_NO_S3[@]}"; do echo "    $s"; done
fi
if [[ ${#ANALYZE_FAILED[@]} -gt 0 ]]; then
    echo "  Analyze errors:                   ${#ANALYZE_FAILED[@]}"
    for s in "${ANALYZE_FAILED[@]}"; do echo "    $s"; done
fi
