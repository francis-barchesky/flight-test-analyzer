#!/usr/bin/env bash
#
# recover_stubs.sh — Targeted re-pull + re-analyze of stubbed sortie dirs.
#
# Why this exists:
#   pipeline.sh's day-by-day filter has been flaky and silently dropped ~85
#   N208B sorties' AFCS_del / AFCS_MiscAnalysis downloads, leaving stub
#   analysis JSONs (3 KB, 0 episodes, 0 mode_transitions).
#
# This script:
#   1) Scans the local data_root for sortie dirs whose analysis JSON is < 20 KB.
#   2) For each stubbed sortie, maps the dir name back to its S3 prefix:
#        dir  "S119_2_N208B"  ->  s3://merlin-pilot-iads-data-exports/N208B/S119N208B_2/bulk_exports/
#        dir  "G026_N208B"    ->  s3://merlin-pilot-iads-data-exports/N208B/G026N208B/bulk_exports/
#   3) Downloads AFCS_del*, AFCS_MiscAnalysis* from bulk_exports into the sortie dir.
#   4) Downloads matching iads_servo_torques_datagroup_<SORTIE>(_<n>)?.zip
#      from s3://merlin-pilot-iads-analysis (flat dump) into the sortie dir.
#   5) Hands the data_root to run_batch.py --zips-only to re-analyze.
#
# Usage:
#   bash recover_stubs.sh                    # run for real
#   bash recover_stubs.sh --dry-run          # list what would happen
#   bash recover_stubs.sh --only S119_2_N208B,S125_N208B   # narrow scope
#
set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="$SCRIPT_DIR"

EXPORTS_BUCKET="s3://merlin-pilot-iads-data-exports"
ANALYSIS_BUCKET="s3://merlin-pilot-iads-analysis"
STUB_SIZE_BYTES=20480   # < 20 KB = stub
DRY_RUN=0
ONLY_LIST=""

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --only)    ;;
        --only=*)  ONLY_LIST="${arg#--only=}" ;;
    esac
done
# Support --only X form
for ((i=1; i<=$#; i++)); do
    if [[ "${!i}" == "--only" ]]; then
        next=$((i+1))
        ONLY_LIST="${!next}"
    fi
done

# ── Detect Python ──────────────────────────────────────────────────────────────
PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
if [[ -x "$PY313" ]]; then
    PYTHON="$PY313"
elif command -v py &>/dev/null; then
    PYTHON="py -3.13"
else
    PYTHON="python"
fi

# ── 1. Find stubbed sortie dirs ────────────────────────────────────────────────
echo "Scanning $DATA_ROOT for stubbed analysis JSONs..."
STUBS=()
for dir in "$DATA_ROOT"/*/; do
    [[ -d "$dir" ]] || continue
    name="$(basename "$dir")"
    [[ "$name" == "."* ]] && continue
    [[ "$name" == "plots" ]] && continue
    [[ "$name" == "__pycache__" ]] && continue
    [[ "$name" != [SG]* ]] && continue   # sortie dirs always start with S or G
    # Skip if --only filter excludes
    if [[ -n "$ONLY_LIST" ]]; then
        if ! [[ ",${ONLY_LIST}," == *",${name},"* ]]; then
            continue
        fi
    fi
    # Find main analysis JSON (exclude _hires) and check size
    main=$(find "$dir" -maxdepth 1 -name 'analysis_*.json' -not -name '*_hires*' 2>/dev/null | head -1)
    [[ -z "$main" ]] && main=$(find "$dir" -maxdepth 1 -name 'analysis.json' 2>/dev/null | head -1)
    if [[ -z "$main" ]]; then
        # No analysis at all = also a candidate
        STUBS+=("$name")
        continue
    fi
    size=$(stat -c%s "$main" 2>/dev/null || stat -f%z "$main" 2>/dev/null || echo 0)
    if (( size < STUB_SIZE_BYTES )); then
        STUBS+=("$name")
    fi
done

if [[ ${#STUBS[@]} -eq 0 ]]; then
    echo "No stubbed sorties found. Nothing to do."
    exit 0
fi

echo "Stubbed sorties: ${#STUBS[@]}"
for s in "${STUBS[@]}"; do echo "  $s"; done
echo

# ── 2. Map dir name -> S3 sortie key ──────────────────────────────────────────
# "S119_2_N208B" -> head="S119"  leg="2"  tail="N208B"  -> S3 sortie = "S119N208B_2"
# "G026_N208B"   -> head="G026"  leg=""   tail="N208B"  -> S3 sortie = "G026N208B"
sortie_to_s3key() {
    local dir="$1"
    local head tail leg s3
    # Split on _ : either 2 parts (head_tail) or 3 parts (head_leg_tail)
    local n_parts
    n_parts=$(echo "$dir" | awk -F_ '{print NF}')
    if [[ "$n_parts" -eq 2 ]]; then
        head="${dir%_*}"
        tail="${dir##*_}"
        leg=""
    elif [[ "$n_parts" -eq 3 ]]; then
        head="$(echo "$dir" | awk -F_ '{print $1}')"
        leg="$(echo "$dir" | awk -F_ '{print $2}')"
        tail="$(echo "$dir" | awk -F_ '{print $3}')"
    else
        echo ""
        return
    fi
    if [[ -n "$leg" ]]; then
        s3="${head}${tail}_${leg}"
    else
        s3="${head}${tail}"
    fi
    echo "$tail|$s3"
}

# ── 3. Cache S3 listings once (not per-day!) ──────────────────────────────────
TMP=$(mktemp -d /tmp/recover_stubs.XXXX)
trap 'rm -rf "$TMP"' EXIT

EXPORTS_LIST="$TMP/exports.ls"
ANALYSIS_LIST="$TMP/analysis.ls"

echo "Listing $EXPORTS_BUCKET ..."
aws s3 ls "$EXPORTS_BUCKET" --recursive > "$EXPORTS_LIST"
echo "  $(wc -l < "$EXPORTS_LIST") objects"

echo "Listing $ANALYSIS_BUCKET ..."
aws s3 ls "$ANALYSIS_BUCKET" --recursive > "$ANALYSIS_LIST"
echo "  $(wc -l < "$ANALYSIS_LIST") objects"
echo

# ── 4. Per-sortie: download → analyze → ZIPs auto-delete → next ───────────────
# This interleaved flow is critical: with delete_zips_after=true in
# batch_config, run_batch.py deletes a sortie's ZIPs as soon as it finishes
# analyzing, so only one sortie's worth of ZIPs (typically <300 MB) lives on
# disk at any moment. Doing all downloads upfront would balloon to 20-50 GB.
TOTAL_DL=0
TOTAL_OK=0
FAILED=()
ANALYZE_FAILED=()
idx=0
for dir_name in "${STUBS[@]}"; do
    idx=$((idx + 1))
    mapping=$(sortie_to_s3key "$dir_name")
    if [[ -z "$mapping" ]]; then
        echo "[$idx/${#STUBS[@]}] $dir_name  SKIP (can't map to S3 key)"
        FAILED+=("$dir_name")
        continue
    fi
    tail="${mapping%|*}"
    s3sortie="${mapping#*|}"
    dest_dir="$DATA_ROOT/$dir_name"

    echo "── [$idx/${#STUBS[@]}] $dir_name → ${tail}/${s3sortie}/ ──"

    # Find bulk_exports candidates: AFCS_del* and AFCS_MiscAnalysis*.
    # Multiple versions often coexist on S3 for the same sortie
    # (e.g. AFCS_del3_v20260323, AFCS_del5_v20260417, AFCS_del5_v20260423).
    # Analyzing them all wastes time and can cross-contaminate via merge —
    # so keep only:
    #   - the single newest AFCS_MiscAnalysis (by v-date)
    #   - the single newest AFCS_del* (highest del#, then latest v-date)
    BULK_KEYS=$(awk -v tail="$tail" -v sortie="$s3sortie" '
        $4 ~ ("^" tail "/" sortie "/bulk_exports/(AFCS_del|AFCS_MiscAnalysis)") &&
        $4 !~ /_test_points\.zip$/ &&
        $4 !~ /_[0-9]{14}\.zip$/ {
            print $4
        }' "$EXPORTS_LIST" | awk -F/ '
        {
            key  = $0
            base = $NF
            # Extract v-date (YYYYMMDD)
            vdate = ""
            if (match(base, /_v[0-9]{8}_/)) {
                vdate = substr(base, RSTART+2, 8)
            }
            if (base ~ /^AFCS_MiscAnalysis/) {
                kind = "MISC"
                delnum = 0
            } else if (match(base, /^AFCS_del[0-9]+/)) {
                kind = "DEL"
                # extract digits after "AFCS_del"
                m = substr(base, RSTART+8, RLENGTH-8)
                delnum = m + 0
            } else {
                next
            }
            # Rank: delnum first, then vdate (string compare OK for YYYYMMDD)
            score = sprintf("%03d_%s", delnum, vdate)
            if (score > best[kind]) {
                best[kind] = score
                pick[kind] = key
            }
        }
        END {
            if (pick["DEL"])  print pick["DEL"]
            if (pick["MISC"]) print pick["MISC"]
        }')

    # Find the SINGLE NEWEST torque ZIP for this sortie tag (sort by S3
    # LastModified descending). After "_<TAG>_", a 1-3 digit segment is a
    # sortie leg number (different sortie dir, reject); 14-digit is a
    # workflow timestamp re-run (keep); alpha is a variant like _ACS (keep).
    TORQUE_KEYS=$(awk -v sortie="$s3sortie" '
        $4 ~ ("^iads_servo_torques_datagroup_" sortie "([._A-Za-z0-9][^/]*)?\\.zip$") &&
        $4 !~ ("^iads_servo_torques_datagroup_" sortie "_[0-9]{1,3}(_|\\.)") &&
        $4 !~ /_test_points/ {
            print $1 " " $2 " " $4
        }' "$ANALYSIS_LIST" | sort -r | head -1 | awk '{print $3}')

    if [[ -z "$BULK_KEYS" && -z "$TORQUE_KEYS" ]]; then
        echo "  no matching files in either bucket"
        FAILED+=("$dir_name")
        continue
    fi

    mkdir -p "$dest_dir"

    # 4a. Download ALL ZIPs for THIS sortie
    n_this=0
    while IFS= read -r key; do
        [[ -z "$key" ]] && continue
        local_file="$dest_dir/$(basename "$key")"
        if [[ -f "$local_file" ]]; then
            echo "  already have $(basename "$key")"
            continue
        fi
        if [[ $DRY_RUN -eq 1 ]]; then
            echo "  [dry-run] would: aws s3 cp $EXPORTS_BUCKET/$key  $local_file"
        else
            echo "  download $EXPORTS_BUCKET/$key"
            aws s3 cp "$EXPORTS_BUCKET/$key" "$local_file" --only-show-errors
            TOTAL_DL=$((TOTAL_DL + 1))
            n_this=$((n_this + 1))
        fi
    done <<< "$BULK_KEYS"

    while IFS= read -r key; do
        [[ -z "$key" ]] && continue
        local_file="$dest_dir/$(basename "$key")"
        if [[ -f "$local_file" ]]; then
            echo "  already have $(basename "$key")"
            continue
        fi
        if [[ $DRY_RUN -eq 1 ]]; then
            echo "  [dry-run] would: aws s3 cp $ANALYSIS_BUCKET/$key  $local_file"
        else
            echo "  download $ANALYSIS_BUCKET/$key"
            aws s3 cp "$ANALYSIS_BUCKET/$key" "$local_file" --only-show-errors
            TOTAL_DL=$((TOTAL_DL + 1))
            n_this=$((n_this + 1))
        fi
    done <<< "$TORQUE_KEYS"

    # 4b. Analyze immediately (relies on --zips-only to filter to this dir,
    #     and delete_zips_after=true to reclaim disk before the next sortie).
    # Analyze if there are ANY ZIPs in the dir, even ones already on disk from
    # a prior interrupted run — otherwise we'd leave them unprocessed forever.
    n_zips_in_dir=$(find "$dest_dir" -maxdepth 1 -name '*.zip' 2>/dev/null | wc -l)
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "  [dry-run] would: $PYTHON run_batch.py --zips-only  (this sortie only, ZIPs deleted after)"
    elif [[ $n_zips_in_dir -eq 0 ]]; then
        echo "  no ZIPs available — skipping analyze"
    else
        echo "  analyze ($n_zips_in_dir ZIP(s) in dir)..."
        if "$PYTHON" "$SCRIPT_DIR/run_batch.py" "$SCRIPT_DIR/batch_config.json" --zips-only; then
            TOTAL_OK=$((TOTAL_OK + 1))
        else
            echo "  ANALYZE FAILED for $dir_name"
            ANALYZE_FAILED+=("$dir_name")
        fi
        # Disk-usage sanity heartbeat
        if command -v du &>/dev/null; then
            disk_used=$(du -sh "$DATA_ROOT" 2>/dev/null | awk '{print $1}')
            echo "  data_root size after this sortie: ${disk_used:-unknown}"
        fi
    fi
done

echo
echo "Recovery summary:"
echo "  Sorties processed (analyze ok): $TOTAL_OK"
echo "  Files downloaded total:         $TOTAL_DL"
if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo "  Skipped (no S3 match):          ${#FAILED[@]}"
    for s in "${FAILED[@]}"; do echo "    $s"; done
fi
if [[ ${#ANALYZE_FAILED[@]} -gt 0 ]]; then
    echo "  Analyze errors:                 ${#ANALYZE_FAILED[@]}"
    for s in "${ANALYZE_FAILED[@]}"; do echo "    $s"; done
fi
