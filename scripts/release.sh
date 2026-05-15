#!/bin/sh
#
# release.sh BUMP
#
#   BUMP ∈ major | minor | patch
#
# Bumps the version derived from the last git tag, renames the
# `## Unreleased` section in CHANGELOG.md to `## X.Y.Z — YYYY-MM-DD`,
# adds a fresh empty `## Unreleased` placeholder, commits, and tags
# the commit `vX.Y.Z`.
#
# Does NOT push to remote and does NOT publish to hex.pm — those steps
# stay manual:
#
#   git push --follow-tags
#   rebar3 hex publish

set -eu

BUMP=${1:-}
case "$BUMP" in
    major|minor|patch) ;;
    *)
        echo "usage: $0 (major|minor|patch)" >&2
        exit 2
        ;;
esac

# 0. Sanity checks.
if [ -n "$(git status --porcelain)" ]; then
    echo "error: working tree not clean — commit or stash first" >&2
    exit 1
fi

if ! grep -q '^## Unreleased' CHANGELOG.md; then
    echo "error: CHANGELOG.md has no '## Unreleased' section" >&2
    exit 1
fi

# Reject empty Unreleased — nothing to release.
UNRELEASED_BODY=$(awk '
    /^## Unreleased/ { in_section = 1; next }
    /^## / && in_section { exit }
    in_section { print }
' CHANGELOG.md | grep -v '^[[:space:]]*$' || true)

if [ -z "$UNRELEASED_BODY" ]; then
    echo "error: '## Unreleased' section is empty — nothing to release" >&2
    exit 1
fi

# 1. Compute new version from last tag.
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
LAST=${LAST_TAG#v}

MAJ=${LAST%%.*}
REST=${LAST#*.}
MIN=${REST%%.*}
PAT=${REST#*.}

case "$BUMP" in
    major) NEW="$((MAJ + 1)).0.0" ;;
    minor) NEW="${MAJ}.$((MIN + 1)).0" ;;
    patch) NEW="${MAJ}.${MIN}.$((PAT + 1))" ;;
esac

DATE=$(date +%F)

echo "Releasing ${LAST} → ${NEW} (${DATE})"

# 2. Rewrite CHANGELOG: rename Unreleased, add fresh empty placeholder.
TMP=$(mktemp)
awk -v ver="$NEW" -v date="$DATE" '
    /^## Unreleased$/ {
        print "## Unreleased"
        print ""
        print "## " ver " — " date
        next
    }
    { print }
' CHANGELOG.md > "$TMP"
mv "$TMP" CHANGELOG.md

# 3. Commit and tag.
git add CHANGELOG.md
git commit -m "Release ${NEW}"
git tag -a "v${NEW}" -m "Release ${NEW}"

echo
echo "Done. To publish:"
echo "  git push --follow-tags"
echo "  rebar3 hex publish"
