#!/usr/bin/env bash
set -euo pipefail

RELEASE_BRANCH="${1:-}"
PUBLISH="${2:-false}"
# The released version, anchored from the release job (prepare-vars.outputs.version)
# rather than read from the branch tip. This keeps the bump idempotent: on an
# overwrite=true re-run of the same release, NEXT_VERSION is still computed from
# the version that was RELEASED, so the branch (already bumped to NEXT) shows no
# diff below and the run no-ops - instead of reading the already-bumped tip and
# advancing a second time. Falls back to the checked-out code when not supplied.
RELEASED_VERSION="${3:-}"

if [[ -z "$RELEASE_BRANCH" ]]; then
	echo "Error: release branch argument required"
	echo "Usage: $0 <release-branch> [publish]"
	exit 1
fi

if [[ "$RELEASE_BRANCH" == "main" ]]; then
	echo "Error: refusing to bump a patch version on main (main bumps happen at cut time via cut.yml / cut-release.sh)"
	exit 1
fi

# Prefer the released version passed by the caller (the idempotent anchor above);
# fall back to the version in the checked-out code when it isn't supplied.
if [[ -n "$RELEASED_VERSION" ]]; then
	VERSION="$RELEASED_VERSION"
	echo "Released version (from caller): ${VERSION}"
else
	VERSION=$(cargo metadata --format-version 1 --no-deps | \
		jq -r '.packages | map(select(.name == "surrealdb"))[0].version')
	echo "Released version (from code): ${VERSION}"
fi

if [[ -z "$VERSION" || "$VERSION" == "null" ]]; then
	echo "Error: Could not determine the released version"
	exit 1
fi

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
# shellcheck disable=SC2086 # PACKAGES is an intentional list of --package args
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
git checkout -B "${PR_BRANCH}"

# Only push and create/update the PR if publishing
if [[ "$PUBLISH" == "true" ]]; then
	# Update the remote PR branch IN PLACE rather than deleting + recreating it,
	# so an existing PR keeps its identity and review state across retries.
	# --force-with-lease, leased to the tip we just observed, makes the overwrite
	# safe (it refuses if the branch moved since); a missing branch is a plain
	# create.
	if remote_sha="$(git ls-remote --exit-code origin "refs/heads/${PR_BRANCH}" 2>/dev/null | cut -f1)"; then
		git push --force-with-lease="${PR_BRANCH}:${remote_sha}" origin "HEAD:${PR_BRANCH}"
	else
		git push origin "HEAD:${PR_BRANCH}"
	fi

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
		PR_URL=$(gh pr view "${existing_pr}" --json url -q '.url')
	else
		# Create PR
		PR_URL=$(gh pr create \
			--base "${RELEASE_BRANCH}" \
			--head "${PR_BRANCH}" \
			--title "${PR_TITLE}" \
			--body "${PR_BODY}")

		echo "Created PR to bump ${RELEASE_BRANCH} to ${NEXT_VERSION}"
	fi

	echo "PR: ${PR_URL}"

	# Track this bump against the milestone for the version it introduces. If the
	# auto-merge below is ever dropped (a PR can fall out of the merge queue), the
	# PR stays visible as open work on the v${NEXT_VERSION} milestone rather than
	# being silently forgotten before the next release. The milestone is created
	# when it does not exist yet. Non-fatal end to end: a milestone hiccup never
	# fails the release.
	MILESTONE_TITLE="v${NEXT_VERSION}"
	REPO="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q '.nameWithOwner')}"
	PR_NUMBER="${PR_URL##*/}"
	milestone_number="$(gh api --paginate "repos/${REPO}/milestones?state=all&per_page=100" \
		--jq ".[] | select(.title == \"${MILESTONE_TITLE}\") | .number" 2>/dev/null | head -n1)"
	if [[ -z "$milestone_number" ]]; then
		milestone_number="$(gh api -X POST "repos/${REPO}/milestones" \
			-f title="${MILESTONE_TITLE}" --jq '.number' 2>/dev/null || true)"
		[[ -n "$milestone_number" ]] && echo "Created milestone ${MILESTONE_TITLE} (#${milestone_number})"
	fi
	if [[ -n "$milestone_number" ]]; then
		if gh api -X PATCH "repos/${REPO}/issues/${PR_NUMBER}" \
			-F milestone="${milestone_number}" >/dev/null 2>&1; then
			echo "Assigned ${PR_URL} to milestone ${MILESTONE_TITLE}"
		else
			echo "::warning title=Milestone::could not assign ${PR_URL} to milestone ${MILESTONE_TITLE}; assign it manually"
		fi
	else
		echo "::warning title=Milestone::could not find or create milestone ${MILESTONE_TITLE}; PR left unassigned"
	fi

	# These version-bump PRs are mechanical and must land before the next
	# release, so merge them as directly as possible. Try an immediate squash
	# merge first - the App is a ruleset bypass actor on this repo, so this
	# lands even while required checks are still queued - then fall back to
	# enabling GitHub auto-merge (which waits for required checks), and only
	# then to a warning. Non-fatal: a merge that cannot proceed leaves the PR
	# open rather than failing the release.
	gh pr merge --squash "${PR_URL}" \
		|| gh pr merge --auto --squash "${PR_URL}" \
		|| echo "::warning title=Auto-merge::could not merge ${PR_URL}; merge it manually"

	# Surface the PR URL as a step output when running in GitHub Actions so the
	# release summary can list it.
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		echo "pr-url=${PR_URL}" >> "$GITHUB_OUTPUT"
	fi
else
	echo "[Dry-run] Would create PR to bump ${RELEASE_BRANCH} to ${NEXT_VERSION}"
fi
