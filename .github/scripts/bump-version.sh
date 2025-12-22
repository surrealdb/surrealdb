#!/usr/bin/env bash
set -e

VERSION="$1"
GITHUB_OUTPUT="${2:-}"

if [[ -z "$VERSION" ]]; then
	echo "Error: Version argument required"
	echo "Usage: $0 <version> [github-output-file]"
	exit 1
fi

RELEASE_BRANCH="release/v${VERSION}"

# Configure git
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Create release branch
git checkout -b "${RELEASE_BRANCH}"

# Bump version in workspace
cargo set-version --workspace "${VERSION}"
# Update lock file (only touch workspace crates, not dependencies)
cargo update -p surrealdb -p surrealdb-core -p surrealdb-server

# Commit changes
git add -A
git commit -m "Prepare v${VERSION} release"

# Push branch (tag will be created later after successful release)
git push origin "${RELEASE_BRANCH}"

# Output the release branch
if [[ -n "$GITHUB_OUTPUT" ]]; then
	echo "release-branch=${RELEASE_BRANCH}" >> "$GITHUB_OUTPUT"
else
	echo "${RELEASE_BRANCH}"
fi

