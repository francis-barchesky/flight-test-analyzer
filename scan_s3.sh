#!/usr/bin/env bash
#
# scan_s3.sh — Search both IADS S3 buckets between two upload dates and
# report which sortie tags have data. Optionally rewrite batch_config.json
# so pipeline.sh runs for that span.
#
# Usage:
#   bash scan_s3.sh <start> <end>                     # scan only
#   bash scan_s3.sh <start> <end> --update-config     # also write dates to batch_config.json
#   bash scan_s3.sh <start> <end> --update-config --tighten
#                                                     # use actual earliest/latest data dates
#   bash scan_s3.sh <start> <end> --refresh           # force re-list S3 (ignore cache)
#
#   <start> and <end> are YYYY-MM-DD, or the literal word "today".
#
# Pattern comes from batch_config.json's "download_pattern" — same source pipeline.sh
# uses, so the scan stays in sync with what the pipeline would actually download.

set -euo pipefail
unset PYTHONHOME PYTHONPATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
CACHE_DIR="${TMPDIR:-/tmp}/scan_s3_cache"
CACHE_TTL_SEC=3600   # reuse cached S3 listings if younger than this

EXPORTS_BUCKET="s3://merlin-pilot-iads-data-exports"
ANALYSIS_BUCKET="s3://merlin-pilot-iads-analysis"

# ── Detect Python (3.13 preferred — 3.12 on PATH causes SRE mismatches) ──────
PY313="/c/Users/FrancisBarchesky/AppData/Local/Programs/Python/Python313/python.exe"
if [[ -n "${PYTHON:-}" ]] && command -v "$PYTHON" &>/dev/null; then
    :
elif [[ -x "$PY313" ]]; then
    PYTHON="$PY313"
elif command -v py &>/dev/null; then
    PYTHON="py -3.13"
else
    PYTHON="python"
fi

# ── Args ──────────────────────────────────────────────────────────────────────
if [[ $# -lt 2 ]]; then
    sed -n '3,20p' "$0"
    exit 1
fi

START_RAW="$1"; shift
END_RAW="$1";   shift
UPDATE_CONFIG=0
TIGHTEN=0
REFRESH=0
for arg in "$@"; do
    case "$arg" in
        --update-config) UPDATE_CONFIG=1 ;;
        --tighten)       TIGHTEN=1 ;;
        --refresh)       REFRESH=1 ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

resolve_date() {
    local v="$1"
    if [[ "$v" == "today" ]]; then date +%Y-%m-%d; else echo "$v"; fi
}
START_DATE="$(resolve_date "$START_RAW")"
END_DATE="$(resolve_date "$END_RAW")"

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
   ! [[ "$END_DATE"   =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got start='$START_DATE' end='$END_DATE')" >&2
    exit 2
fi

# ── Read download_pattern from batch_config.json ──────────────────────────────
CONFIG_WIN="$CONFIG"
if command -v cygpath &>/dev/null; then CONFIG_WIN="$(cygpath -w "$CONFIG")"; fi

PATTERN=$("$PYTHON" -c "
import json
c = json.load(open(r'$CONFIG_WIN'))
p = c.get('download_pattern', '*')
if isinstance(p, list):
    p = '(' + '|'.join(str(x) for x in p) + ')'
print(p)
")

# ── Cache S3 listings ─────────────────────────────────────────────────────────
mkdir -p "$CACHE_DIR"
EXPORTS_LIST="$CACHE_DIR/exports.txt"
ANALYSIS_LIST="$CACHE_DIR/analysis.txt"

is_fresh() {
    local f="$1"
    [[ -f "$f" ]] || return 1
    local age=$(( $(date +%s) - $(stat -c%Y "$f" 2>/dev/null || stat -f%m "$f" 2>/dev/null || echo 0) ))
    (( age < CACHE_TTL_SEC ))
}

if (( REFRESH )) || ! is_fresh "$EXPORTS_LIST"; then
    echo "Listing $EXPORTS_BUCKET ..." >&2
    aws s3 ls "$EXPORTS_BUCKET" --recursive > "$EXPORTS_LIST"
else
    age_min=$(( ( $(date +%s) - $(stat -c%Y "$EXPORTS_LIST" 2>/dev/null || stat -f%m "$EXPORTS_LIST" 2>/dev/null) ) / 60 ))
    echo "Using cached exports listing (${age_min}m old)" >&2
fi

if (( REFRESH )) || ! is_fresh "$ANALYSIS_LIST"; then
    echo "Listing $ANALYSIS_BUCKET ..." >&2
    aws s3 ls "$ANALYSIS_BUCKET" --recursive > "$ANALYSIS_LIST"
else
    age_min=$(( ( $(date +%s) - $(stat -c%Y "$ANALYSIS_LIST" 2>/dev/null || stat -f%m "$ANALYSIS_LIST" 2>/dev/null) ) / 60 ))
    echo "Using cached analysis listing (${age_min}m old)" >&2
fi

# ── Filter & build report ────────────────────────────────────────────────────
TMP=$(mktemp -d "$CACHE_DIR/run.XXXX")
MATCHES="$TMP/matches.txt"

awk -v start="$START_DATE" -v end="$END_DATE" -v pattern="$PATTERN" '
  $1 >= start && $1 <= end &&
  $4 ~ ("(^|/)" pattern "[^/]*$") &&
  $4 !~ /test_point_exports/ &&
  $4 !~ /_test_points\.zip$/ &&
  $4 !~ /_[0-9]{14}\.zip$/ {
    fname = $4; sub(/.*\//,"",fname)
    if (match(fname, /_v[0-9]+_/)) {
        tag = substr(fname, RSTART+RLENGTH); sub(/\.zip$/,"",tag)
    } else if (fname ~ /^iads_servo_torques_datagroup_/) {
        tag = fname; sub(/^iads_servo_torques_datagroup_/,"",tag); sub(/\.zip$/,"",tag)
    } else { next }
    print $1, tag, fname
  }
' "$EXPORTS_LIST" "$ANALYSIS_LIST" > "$MATCHES"

TOTAL_FILES=$(wc -l < "$MATCHES" | tr -d ' ')

echo
echo "=== S3 Sortie Scan ==="
echo "  start   : $START_DATE"
echo "  end     : $END_DATE"
echo "  pattern : $PATTERN"
echo "  matches : $TOTAL_FILES file(s)"
echo

if (( TOTAL_FILES == 0 )); then
    echo "  (no matching uploads in this window)"
else
    echo "=== Days with data ==="
    awk '{print $1, $2}' "$MATCHES" \
      | sort -u \
      | awk '{
          dates[$1]++
          if (tags[$1]) tags[$1] = tags[$1] ", " $2
          else          tags[$1] = $2
        }
        END {
          for (d in dates) print d, dates[d], tags[d]
        }' \
      | sort \
      | awk -v OFS='' '{
          d=$1; n=$2;
          $1=""; $2="";
          sub(/^  /, "", $0);
          printf "  %s : %3d sortie tag(s) — %s\n", d, n, $0
        }'

    EARLIEST=$(awk '{print $1}' "$MATCHES" | sort -u | head -1)
    LATEST=$(  awk '{print $1}' "$MATCHES" | sort -u | tail -1)
    UNIQ_TAGS=$(awk '{print $2}' "$MATCHES" | sort -u | wc -l | tr -d ' ')

    echo
    echo "=== Summary ==="
    echo "  unique sortie tags : $UNIQ_TAGS"
    echo "  total files        : $TOTAL_FILES"
    echo "  earliest upload    : $EARLIEST"
    echo "  latest upload      : $LATEST"
fi

# ── Optional config update ────────────────────────────────────────────────────
if (( UPDATE_CONFIG )); then
    if (( TOTAL_FILES == 0 )) && (( TIGHTEN )); then
        echo
        echo "Refusing --tighten with zero matches; not modifying config." >&2
        rm -rf "$TMP"
        exit 3
    fi

    if (( TIGHTEN )); then
        NEW_START="$EARLIEST"
        NEW_END="$LATEST"
    else
        NEW_START="$START_DATE"
        NEW_END="$END_DATE"
    fi

    echo
    echo "=== Updating batch_config.json ==="
    "$PYTHON" - <<PYEOF
import json, pathlib
p = pathlib.Path(r'$CONFIG_WIN')
c = json.loads(p.read_text())
old_start = c.get('download_start_date')
old_end   = c.get('download_end_date')
c['download_start_date'] = '$NEW_START'
c['download_end_date']   = '$NEW_END'
p.write_text(json.dumps(c, indent=2))
print(f"  download_start_date: {old_start} -> {c['download_start_date']}")
print(f"  download_end_date  : {old_end} -> {c['download_end_date']}")
PYEOF
    echo
    echo "Next: bash pipeline.sh"
fi

rm -rf "$TMP"
