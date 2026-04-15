#!/usr/bin/env bash
#
# publish_data.sh — Push analysis JSONs to the data branch (force-push, no history)
#
# Usage:
#   bash publish_data.sh              # publish from current data_root (batch_config.json)
#   bash publish_data.sh --dry-run    # preview without pushing
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/batch_config.json"
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        *.json)    CONFIG="$(realpath "$arg")" ;;
    esac
done

# ── Read data_root from config ─────────────────────────────────────────────────
if command -v python3 &>/dev/null; then PYTHON=python3
elif command -v python &>/dev/null; then PYTHON=python
else echo "ERROR: Python not found"; exit 1
fi

DATA_ROOT="$("$PYTHON" -c "
import json, os
c = json.load(open(r'$CONFIG'))
d = c.get('data_root', '.')
print(d if os.path.isabs(d) else os.path.normpath(os.path.join(os.path.dirname(r'$CONFIG'), d)))
")"

# ── Collect JSONs ──────────────────────────────────────────────────────────────
mapfile -t JSONS < <(find "$DATA_ROOT" -maxdepth 2 -name "analysis_*.json" | sort)

if [[ ${#JSONS[@]} -eq 0 ]]; then
    echo "No analysis_*.json files found under $DATA_ROOT"
    exit 0
fi

echo "Found ${#JSONS[@]} JSON file(s):"
for j in "${JSONS[@]}"; do
    size=$(du -h "$j" | cut -f1)
    echo "  $size  $(basename "$j")"
done
echo

if [[ $DRY_RUN -eq 1 ]]; then
    echo "[dry-run] would force-push the above to origin/data"
    exit 0
fi

# ── Use a git worktree so we never leave master ────────────────────────────────
WORKTREE="$(mktemp -d)"
trap 'git -C "$SCRIPT_DIR" worktree remove --force "$WORKTREE" 2>/dev/null || true; rm -rf "$WORKTREE"' EXIT

git -C "$SCRIPT_DIR" worktree add --track -b data-publish "$WORKTREE" origin/data 2>/dev/null \
    || git -C "$SCRIPT_DIR" worktree add "$WORKTREE" origin/data

# ── Copy JSONs into worktree ───────────────────────────────────────────────────
for j in "${JSONS[@]}"; do
    cp "$j" "$WORKTREE/$(basename "$j")"
done

# ── Commit and force-push ──────────────────────────────────────────────────────
cd "$WORKTREE"
git add .
if git diff --cached --quiet; then
    echo "No changes to publish — data branch already up to date."
    exit 0
fi

TIMESTAMP="$(date '+%Y-%m-%d %H:%M')"
N="${#JSONS[@]}"
git commit -m "data: ${N} sortie(s) — ${TIMESTAMP}"
git push origin HEAD:data

echo
echo "Published ${N} JSON(s) to origin/data"
echo "  https://github.com/$(git remote get-url origin | sed 's|.*github.com/||;s|\.git||')/tree/data"
