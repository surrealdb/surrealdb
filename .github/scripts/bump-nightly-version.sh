#!/usr/bin/env bash
set -e

PUBLISH="${1:-false}"
MAIN_VERSION_INPUT="${2:-}"

# The released version is taken from the code on the currently checked-out
# git ref, not passed in as an argument.
VERSION=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages | map(select(.name == "surrealdb"))[0].version')

if [[ -z "$VERSION" || "$VERSION" == "null" ]]; then
	echo "Error: Could not determine the released version from the code"
	exit 1
fi

echo "Released version (from code): ${VERSION}"

# Determine the appropriate version for main
if [[ -n "$MAIN_VERSION_INPUT" ]]; then
	# User specified exact version for main
	MAIN_VERSION="$MAIN_VERSION_INPUT"
	echo "Using specified main version: ${MAIN_VERSION}"
else
	# Auto-calculate based on release version
	major=$(echo $VERSION | tr "." "\n" | sed -n 1p)
	minor=$(echo $VERSION | tr "." "\n" | sed -n 2p)
	patch=$(echo $VERSION | tr "." "\n" | sed -n 3p)

	# Only a stable x.y.0 release moves main forward (to the next minor nightly).
	# Pre-releases (e.g. 3.0.0-beta.1) and patch releases (e.g. 3.0.1) never change
	# main - main always stays on its -nightly development version.
	if [[ "$patch" == "0" ]] && [[ ! "$VERSION" =~ - ]]; then
		# Stable x.y.0 release -> bump main to next minor nightly
		next_minor=$((minor + 1))
		MAIN_VERSION="${major}.${next_minor}.0-nightly"
		echo "Stable release: auto-bumping main from ${VERSION} to ${MAIN_VERSION} for next development cycle"
	else
		echo "Release ${VERSION} does not change the main branch version (main stays on -nightly); nothing to do"
		exit 0
	fi
fi

# Configure git identity for the commit (fresh runners have none)
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Fetch and check out main (may not exist locally if we're on a patch branch)
git fetch origin main
git checkout main
git pull origin main

# Dynamically build list of surrealdb-* packages (excludes surrealism-*)
PACKAGES=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages[].name' | \
	grep '^surrealdb' | \
	sed 's/^/--package /' | \
	tr '\n' ' ')

# Bump version for surrealdb packages only
cargo set-version $PACKAGES "${MAIN_VERSION}"
cargo update -p surrealdb -p surrealdb-core -p surrealdb-server

# Commit changes only if there are any (idempotency)
if git diff --quiet; then
	echo "No version changes detected - version already set to ${MAIN_VERSION}"
	echo "Script completed successfully - no version update needed"
	exit 0
else
	git commit -am "Bump version to ${MAIN_VERSION}"
fi

# Create a branch for the PR
PR_BRANCH="dev/ci/v${MAIN_VERSION}"

# Delete PR branch if it exists (idempotency)
if git ls-remote --exit-code --heads origin "${PR_BRANCH}" >/dev/null 2>&1; then
	echo "PR branch ${PR_BRANCH} already exists, deleting it"
	git push origin --delete "${PR_BRANCH}" || true
fi
if git show-ref --verify --quiet "refs/heads/${PR_BRANCH}"; then
	git branch -D "${PR_BRANCH}"
fi

git checkout -b "${PR_BRANCH}"

# Only push and create PR if publishing
if [[ "$PUBLISH" == "true" ]]; then
	git push origin "${PR_BRANCH}"

	# Define PR title and body (avoid duplication)
	PR_TITLE="Bump version to ${MAIN_VERSION}"
	PR_BODY="Automated version bump to v${MAIN_VERSION} following release v${VERSION}.

**This PR updates the main branch version for the next development cycle.**

- Release version: \`${VERSION}\`
- Main branch version: \`${MAIN_VERSION}\`

Review and merge this PR to prepare main for the next phase of development."

	# Check if PR already exists
	existing_pr=$(gh pr list --head "${PR_BRANCH}" --base main --json number -q '.[0].number' 2>/dev/null || echo "")

	if [[ -n "$existing_pr" ]]; then
		echo "PR #${existing_pr} already exists, updating it"
		gh pr edit "${existing_pr}" \
			--title "${PR_TITLE}" \
			--body "${PR_BODY}"
	else
		# Create PR
		gh pr create \
			--base main \
			--head "${PR_BRANCH}" \
			--title "${PR_TITLE}" \
			--body "${PR_BODY}"

		echo "Created PR to update main branch to ${MAIN_VERSION}"
	fi
else
	echo "[Dry-run] Would create PR to update main branch to ${MAIN_VERSION}"
fi
