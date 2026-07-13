#!/usr/bin/env bash
# reanalyze_ra_sorties.sh
#
# For S150, S151_1, S152:
#   1. Pull iads_radar_altitude_datagroup ZIP from S3 (if not already present).
#   2. Delete existing analysis JSONs so run_batch.py does a full write
#      (not the merge path that preserves old hires data).
#   3. Re-run run_batch.py — skip_existing skips everything that still has
#      a JSON, so only these three sorties get processed.
#
# Run from the flight-test-analyzer/ directory.
# Requires an active AWS SSO session (run: aws sso login).

set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
ANALYSIS_BUCKET="s3://merlin-pilot-iads-analysis"

echo "========================================================"
echo "  RA sortie re-analysis: S150 / S151_1 / S152"
echo "========================================================"

# ── Step 1: Pull radar altitude ZIPs from S3 ─────────────────────────────────
# S150 already has its ZIP locally; include it here only for completeness
# (aws s3 cp won't error if the file already exists — it just overwrites).
declare -A RA_TAGS=(
    ["S150_N208B"]="S150N208B"
    ["S151_1_N208B"]="S151N208B_1"
    ["S152_N208B"]="S152N208B"
)

echo
echo "── Checking / downloading radar altitude ZIPs ──────────────────────────"

# One S3 listing, reused for all sorties
S3_LIST=$(aws s3 ls "$ANALYSIS_BUCKET/" --recursive)

for DIR in "S150_N208B" "S151_1_N208B" "S152_N208B"; do
    TAG="${RA_TAGS[$DIR]}"
    DEST="$SCRIPT_DIR/$DIR/iads_radar_altitude_datagroup_${TAG}.zip"

    if [[ -f "$DEST" ]]; then
        echo "  [$DIR] radar altitude ZIP already present — skipping download"
        continue
    fi

    S3_KEY=$(echo "$S3_LIST" \
        | awk -v t="$TAG" '$4 ~ ("(^|/)iads_radar_altitude_datagroup_" t "\\.zip$") {print $4}' \
        | head -1)

    if [[ -z "$S3_KEY" ]]; then
        echo "  [$DIR] WARNING: no radar altitude ZIP for $TAG found in S3 yet."
        echo "           Wait for IADS ops to finish uploading, then re-run."
    else
        echo "  [$DIR] Downloading s3://.../$S3_KEY"
        aws s3 cp "$ANALYSIS_BUCKET/$S3_KEY" "$DEST"
        echo "  [$DIR] Done."
    fi
done

# ── Step 2: Delete existing analysis JSONs ────────────────────────────────────
echo
echo "── Removing existing analysis JSONs ────────────────────────────────────"

for DIR in "S150_N208B" "S151_1_N208B" "S152_N208B"; do
    SORTIE="${DIR%%_N208B*}"    # S150, S151_1, S152
    JSON="$SCRIPT_DIR/$DIR/analysis_${SORTIE}.json"
    HIRES="$SCRIPT_DIR/$DIR/analysis_${SORTIE}_hires.json"
    [[ -f "$JSON"  ]] && { echo "  Removing $DIR/analysis_${SORTIE}.json";       rm "$JSON";  }
    [[ -f "$HIRES" ]] && { echo "  Removing $DIR/analysis_${SORTIE}_hires.json"; rm "$HIRES"; }
done

# ── Step 3: Re-analyze ────────────────────────────────────────────────────────
echo
echo "── Re-analyzing (skip_existing skips everything else) ──────────────────"
"$PYTHON" "$SCRIPT_DIR/run_batch.py" "$SCRIPT_DIR/batch_config.json"

echo
echo "========================================================"
echo "  Done. Next steps:"
echo "    1. Add S151_1 and S152 to SORTIES in eval/_variant_realdata.py"
echo "    2. python eval/_ra1_vs_ra2_noise_diag.py"
echo "    3. python eval/_ra_per_source_noise_floor.py"
echo "========================================================"
