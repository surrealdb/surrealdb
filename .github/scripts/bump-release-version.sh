#!/usr/bin/env bash
set -e

RELEASE_BRANCH="$1"
PUBLISH="${2:-false}"

if [[ -z "$RELEASE_BRANCH" ]]; then
	echo "Error: release branch argument required"
	echo "Usage: $0 <release-branch> [publish]"
	exit 1
fi

if [[ "$RELEASE_BRANCH" == "main" ]]; then
	echo "Error: refusing to bump a patch version on main (use bump-nightly-version.sh instead)"
	exit 1
fi

# The released version is taken from the code on the currently checked-out
# git ref (the release branch), not passed in as an argument.
VERSION=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages | map(select(.name == "surrealdb"))[0].version')

if [[ -z "$VERSION" || "$VERSION" == "null" ]]; then
	echo "Error: Could not determine the released version from the code"
	exit 1
fi

echo "Released version (from code): ${VERSION}"

# Compute the next version:
#   * stable X.Y.Z            -> X.Y.(Z+1)         (e.g. 3.1.3        -> 3.1.4)
#   * pre-release X.Y.Z-pre.N -> X.Y.Z-pre.(N+1)   (e.g. 3.2.0-beta.1 -> 3.2.0-beta.2)
# Pre-releases keep their base version and label and only increment the trailing
# pre-release number - they are NOT finalised to the stable release. SemVer allows
# several pre-release shapes, all of which are handled below:
#   * dotted numeric tail:  3.2.0-beta.1 -> 3.2.0-beta.2 ; 3.0.0-rc.3 -> 3.0.0-rc.4
#   * attached numeric tail: 3.0.0-rc1   -> 3.0.0-rc2
#   * purely numeric:        3.0.0-1     -> 3.0.0-2
#   * no numeric tail:       3.0.0-rc    -> 3.0.0-rc.1  (start a dotted counter)
if [[ "$VERSION" == *-* ]]; then
	base="${VERSION%%-*}"
	pre="${VERSION#*-}"
	if [[ "$pre" =~ ^(.*[^0-9])([0-9]+)$ ]]; then
		# Label/prefix (dotted like "beta." or attached like "rc") + trailing number
		pre_label="${BASH_REMATCH[1]}"
		pre_num="${BASH_REMATCH[2]}"
		NEXT_VERSION="${base}-${pre_label}$((pre_num + 1))"
	elif [[ "$pre" =~ ^[0-9]+$ ]]; then
		# Entirely numeric pre-release identifier
		NEXT_VERSION="${base}-$((pre + 1))"
	else
		# No numeric identifier to increment (e.g. 3.0.0-rc) -> start a dotted counter
		NEXT_VERSION="${base}-${pre}.1"
	fi
else
	major="${VERSION%%.*}"
	rest="${VERSION#*.}"
	minor="${rest%%.*}"
	patch="${rest##*.}"
	NEXT_VERSION="${major}.${minor}.$((patch + 1))"
fi

echo "Bumping ${RELEASE_BRANCH} from ${VERSION} to ${NEXT_VERSION} for the next patch cycle"

# Configure git identity for the commit (fresh runners have none)
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Fetch and check out the release branch with the latest changes
git fetch origin "${RELEASE_BRANCH}"
git checkout "${RELEASE_BRANCH}"
git pull origin "${RELEASE_BRANCH}"

# Dynamically build list of surrealdb-* packages (excludes surrealism-*)
PACKAGES=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages[].name' | \
	grep '^surrealdb' | \
	sed 's/^/--package /' | \
	tr '\n' ' ')

# Bump version for surrealdb packages only
cargo set-version $PACKAGES "${NEXT_VERSION}"
cargo update -p surrealdb -p surrealdb-core -p surrealdb-server

# Commit changes only if there are any (idempotency)
if git diff --quiet; then
	echo "No version changes detected - version already set to ${NEXT_VERSION}"
	echo "Script completed successfully - no version update needed"
	exit 0
else
	git commit -am "Bump version to ${NEXT_VERSION}"
fi

# Create a branch for the PR
PR_BRANCH="dev/ci/v${NEXT_VERSION}"

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
	PR_TITLE="Bump version to ${NEXT_VERSION}"
	PR_BODY="Automated version bump to v${NEXT_VERSION} following release v${VERSION}.

**This PR updates the \`${RELEASE_BRANCH}\` release branch to the next patch version.**

- Release version: \`${VERSION}\`
- Next patch version: \`${NEXT_VERSION}\`

Review and merge this PR to prepare \`${RELEASE_BRANCH}\` for the next patch release."

	# Check if PR already exists
	existing_pr=$(gh pr list --head "${PR_BRANCH}" --base "${RELEASE_BRANCH}" --json number -q '.[0].number' 2>/dev/null || echo "")

	if [[ -n "$existing_pr" ]]; then
		echo "PR #${existing_pr} already exists, updating it"
		gh pr edit "${existing_pr}" \
			--title "${PR_TITLE}" \
			--body "${PR_BODY}"
	else
		# Create PR
		gh pr create \
			--base "${RELEASE_BRANCH}" \
			--head "${PR_BRANCH}" \
			--title "${PR_TITLE}" \
			--body "${PR_BODY}"

		echo "Created PR to bump ${RELEASE_BRANCH} to ${NEXT_VERSION}"
	fi
else
	echo "[Dry-run] Would create PR to bump ${RELEASE_BRANCH} to ${NEXT_VERSION}"
fi
