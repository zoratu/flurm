#!/bin/bash
###############################################################################
# check-slurm-testsuite-updates.sh
#
# Checks the SchedMD SLURM testsuite for updates since the last check.
# Compares the current testsuite commit SHA against a stored reference.
#
# Usage:
#   ./scripts/check-slurm-testsuite-updates.sh          # Check for updates
#   ./scripts/check-slurm-testsuite-updates.sh --update  # Update the reference SHA
#   ./scripts/check-slurm-testsuite-updates.sh --diff    # Show what changed
#
# Requires: git, curl (or gh CLI)
#
# This script is designed to be run:
#   - Manually before releases
#   - In CI/CD pipelines
#   - As a periodic cron job
###############################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REF_FILE="$PROJECT_DIR/.slurm-testsuite-ref"
SLURM_REPO="SchedMD/slurm"
TESTSUITE_PATH="testsuite"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

get_latest_sha() {
    # Get the latest commit SHA for the testsuite directory
    if command -v gh &>/dev/null; then
        gh api "repos/$SLURM_REPO/commits?path=$TESTSUITE_PATH&per_page=1" \
            --jq '.[0].sha' 2>/dev/null
    else
        curl -s "https://api.github.com/repos/$SLURM_REPO/commits?path=$TESTSUITE_PATH&per_page=1" \
            | grep '"sha"' | head -1 | sed 's/.*"sha": "\([^"]*\)".*/\1/'
    fi
}

get_latest_info() {
    if command -v gh &>/dev/null; then
        gh api "repos/$SLURM_REPO/commits?path=$TESTSUITE_PATH&per_page=5" \
            --jq '.[] | "\(.sha[0:8]) \(.commit.message | split("\n")[0]) (\(.commit.author.date[0:10]))"' 2>/dev/null
    else
        curl -s "https://api.github.com/repos/$SLURM_REPO/commits?path=$TESTSUITE_PATH&per_page=5" \
            | python3 -c "
import json, sys
data = json.load(sys.stdin)
for c in data:
    sha = c['sha'][:8]
    msg = c['commit']['message'].split('\n')[0][:60]
    date = c['commit']['author']['date'][:10]
    print(f'{sha} {msg} ({date})')
" 2>/dev/null
    fi
}

get_diff_since() {
    local since_sha="$1"
    if command -v gh &>/dev/null; then
        gh api "repos/$SLURM_REPO/compare/${since_sha}...master" \
            --jq '.files[] | select(.filename | startswith("testsuite/")) | "\(.status)\t\(.filename)"' 2>/dev/null
    else
        curl -s "https://api.github.com/repos/$SLURM_REPO/compare/${since_sha}...master" \
            | python3 -c "
import json, sys
data = json.load(sys.stdin)
for f in data.get('files', []):
    if f['filename'].startswith('testsuite/'):
        print(f\"{f['status']}\t{f['filename']}\")
" 2>/dev/null
    fi
}

case "${1:-}" in
    --update)
        echo -e "${CYAN}Updating SLURM testsuite reference SHA...${NC}"
        SHA=$(get_latest_sha)
        if [ -n "$SHA" ]; then
            echo "$SHA" > "$REF_FILE"
            echo -e "${GREEN}Reference updated to: $SHA${NC}"
            echo ""
            echo "Recent testsuite commits:"
            get_latest_info
        else
            echo -e "${RED}Failed to fetch latest SHA${NC}"
            exit 1
        fi
        ;;

    --diff)
        if [ ! -f "$REF_FILE" ]; then
            echo -e "${YELLOW}No reference SHA found. Run with --update first.${NC}"
            exit 1
        fi
        REF_SHA=$(cat "$REF_FILE")
        echo -e "${CYAN}Changes since $REF_SHA:${NC}"
        echo ""
        CHANGES=$(get_diff_since "$REF_SHA")
        if [ -z "$CHANGES" ]; then
            echo -e "${GREEN}No changes to testsuite since last check.${NC}"
        else
            echo "$CHANGES"
            echo ""
            # Count by category
            EXPECT_CHANGES=$(echo "$CHANGES" | grep "testsuite/expect/" | wc -l)
            PYTHON_CHANGES=$(echo "$CHANGES" | grep "testsuite/python/" | wc -l)
            OTHER_CHANGES=$(echo "$CHANGES" | grep -v "testsuite/expect/\|testsuite/python/" | wc -l)
            echo -e "Expect tests changed: ${YELLOW}$EXPECT_CHANGES${NC}"
            echo -e "Python tests changed: ${YELLOW}$PYTHON_CHANGES${NC}"
            echo -e "Other changes: ${YELLOW}$OTHER_CHANGES${NC}"

            # Highlight relevant test files for FLURM
            echo ""
            echo -e "${CYAN}Tests relevant to FLURM (core CLI):${NC}"
            echo "$CHANGES" | grep -E "test(1|2|4|5|6|17)\." | head -20 || echo "  (none)"
            echo "$CHANGES" | grep -E "test_(10[6-8]|111|114|116|122)" | head -20 || echo "  (none from Python suite)"
        fi
        ;;

    *)
        # Default: check for updates
        LATEST_SHA=$(get_latest_sha)
        if [ -z "$LATEST_SHA" ]; then
            echo -e "${RED}Failed to fetch latest SHA from GitHub${NC}"
            exit 1
        fi

        if [ ! -f "$REF_FILE" ]; then
            echo -e "${YELLOW}No reference SHA found.${NC}"
            echo "Latest testsuite commit: $LATEST_SHA"
            echo ""
            echo "Run with --update to set the reference point."
            echo ""
            echo "Recent testsuite commits:"
            get_latest_info
            exit 0
        fi

        REF_SHA=$(cat "$REF_FILE")

        if [ "$LATEST_SHA" = "$REF_SHA" ]; then
            echo -e "${GREEN}SLURM testsuite is up to date.${NC}"
            echo "Reference: $REF_SHA"
            exit 0
        else
            echo -e "${YELLOW}SLURM testsuite has been updated!${NC}"
            echo ""
            echo "  Reference: $REF_SHA"
            echo "  Latest:    $LATEST_SHA"
            echo ""
            echo "Recent testsuite commits:"
            get_latest_info
            echo ""
            echo -e "Run ${CYAN}$0 --diff${NC} to see what changed."
            echo -e "Run ${CYAN}$0 --update${NC} after reviewing changes."
            exit 2  # Non-zero to signal updates available
        fi
        ;;
esac
