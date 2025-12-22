#!/usr/bin/env bash
set -e

VERSION="$1"
PUBLISH="${2:-false}"
MAIN_VERSION_INPUT="${3:-}"

if [[ -z "$VERSION" ]]; then
	echo "Error: Version argument required"
	echo "Usage: $0 <version> [publish] [main-version]"
	exit 1
fi

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

	# Check if this is a stable x.y.0 release (no hyphen = no pre-release)
	if [[ "$patch" == "0" ]] && [[ ! "$VERSION" =~ - ]]; then
		# Stable x.y.0 release -> bump main to next minor alpha
		next_minor=$((minor + 1))
		MAIN_VERSION="${major}.${next_minor}.0-alpha"
		echo "Stable release: auto-bumping main from ${VERSION} to ${MAIN_VERSION} for next development cycle"
	elif [[ "$VERSION" =~ - ]]; then
		# Pre-release (contains hyphen) -> strip to first 3 parts (e.g., 3.0.0-beta.1 -> 3.0.0-beta)
		MAIN_VERSION="${major}.${minor}.${patch}"
		echo "Pre-release: auto-stripping patch from ${VERSION} to ${MAIN_VERSION} for main branch"
	else
		# Other stable releases (e.g., 3.0.1) - use as-is
		MAIN_VERSION="$VERSION"
		echo "Using full version ${MAIN_VERSION} for main branch"
	fi
fi

# Check out main and update version
git checkout main
git pull origin main

# Bump version to the main-appropriate version
cargo set-version --workspace "${MAIN_VERSION}"
cargo update -p surrealdb -p surrealdb-core -p surrealdb-server

# Commit changes locally (always test this logic)
git add -A
git commit -m "chore: bump version to ${MAIN_VERSION}"

# Create a branch for the PR
PR_BRANCH="chore/bump-main-to-v${MAIN_VERSION}"

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

