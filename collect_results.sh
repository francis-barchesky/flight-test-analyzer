#!/usr/bin/env bash
#
# collect_results.sh — Copy analysis*.json.gz files from parallel pipeline chunk
# data_roots into the canonical sortie dirs at the repo root.
#
# Copies only if the chunk file is newer than any existing file in the destination
# (safe to re-run). Run this after run_parallel_pipeline.sh completes.
#
# Usage:
#   bash collect_results.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHUNKS_DIR="$SCRIPT_DIR/pipeline_chunks"

COPIED=0
SKIPPED=0
MISSING=0

echo "Collecting results from $CHUNKS_DIR..."
echo

for data_dir in "$CHUNKS_DIR"/data_*/; do
    [[ -d "$data_dir" ]] || continue
    for sortie_dir in "$data_dir"*/; do
        [[ -d "$sortie_dir" ]] || continue
        sortie_name=$(basename "$sortie_dir")
        dest="$SCRIPT_DIR/$sortie_name"

        for analysis_file in "$sortie_dir"analysis*.json.gz; do
            [[ -f "$analysis_file" ]] || continue
            fname=$(basename "$analysis_file")

            if [[ ! -d "$dest" ]]; then
                echo "  WARN  no canonical dir for $sortie_name"
                MISSING=$((MISSING + 1))
                continue
            fi

            dest_file="$dest/$fname"
            if [[ -f "$dest_file" && ! "$analysis_file" -nt "$dest_file" ]]; then
                SKIPPED=$((SKIPPED + 1))
            else
                cp "$analysis_file" "$dest_file"
                echo "  copy  $sortie_name/$fname"
                COPIED=$((COPIED + 1))
            fi
        done
    done
done

echo
echo "Done: $COPIED copied/updated, $SKIPPED already up-to-date, $MISSING missing canonical dirs."
