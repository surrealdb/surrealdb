#!/usr/bin/env bash
set -euo pipefail

# Cut a new release line from main.
#
# main is always `X.Y.0-nightly` (the next minor's development version). Cutting
# is a source-mutating operation that produces commits the rolling build then
# turns into promotable artifacts; it never promotes anything itself.
#
#   minor cut (branch at the END of the X.Y dev cycle):
#     * create `releases/X.Y` from main's current commit, set its version to
#       `X.Y.0-beta.1`, and push it (a new release branch starts at beta);
#     * open a PR bumping main to `X.(Y+1).0-nightly` (main moves on immediately).
#
#   major cut (branch at the START of the (X+1).0 cycle):
#     * create `releases/(X+1).0` from main's current commit, set its version to
#       `(X+1).0.0-alpha.1`, and push it (a new release branch starts at alpha);
#     * open a PR bumping main to `(X+1).0.0-nightly`. main stays on that version
#       through the alpha/beta/rc cycle and only advances to `(X+1).1.0-nightly`
#       once `(X+1).0.0` ships stable (see advance-main-after-release.sh).
#
# Usage: cut-release.sh <minor|major> [publish]
#   publish=false performs a dry-run (no pushes, no PRs).

CUT_TYPE="${1:-}"
PUBLISH="${2:-false}"

if [[ "$CUT_TYPE" != "minor" && "$CUT_TYPE" != "major" ]]; then
	echo "Error: cut type must be 'minor' or 'major', got '${CUT_TYPE}'"
	exit 1
fi

# The cut is always taken from the currently checked-out commit (a commit on main).
VERSION=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages | map(select(.name == "surrealdb"))[0].version')

if [[ -z "$VERSION" || "$VERSION" == "null" ]]; then
	echo "Error: could not determine the main version from the code"
	exit 1
fi

echo "Cutting a ${CUT_TYPE} release from main (version: ${VERSION})"

# main must be on its development version: X.Y.0-nightly.
if [[ ! "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.0-nightly$ ]]; then
	echo "Error: expected main to be on X.Y.0-nightly, got '${VERSION}'"
	echo "Cuts can only be made from main's development version."
	exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"

if [[ "$CUT_TYPE" == "minor" ]]; then
	# Branched at the end of the X.Y dev cycle (beta); main moves on immediately.
	RELEASE_BRANCH="releases/${MAJOR}.${MINOR}"
	BRANCH_VERSION="${MAJOR}.${MINOR}.0-beta.1"
	MAIN_VERSION="${MAJOR}.$((MINOR + 1)).0-nightly"
else
	# Branched at the start of the (X+1).0 cycle (alpha); main parks on the new
	# major and only advances to (X+1).1.0-nightly once (X+1).0.0 ships stable.
	RELEASE_BRANCH="releases/$((MAJOR + 1)).0"
	BRANCH_VERSION="$((MAJOR + 1)).0.0-alpha.1"
	MAIN_VERSION="$((MAJOR + 1)).0.0-nightly"
fi

echo "Main will be bumped to: ${MAIN_VERSION}"
[[ -n "$RELEASE_BRANCH" ]] && echo "Release branch ${RELEASE_BRANCH} will start at: ${BRANCH_VERSION}"

# Configure git identity for the commits (fresh runners have none)
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# The commit we cut from (main HEAD).
BASE_SHA=$(git rev-parse HEAD)

set_workspace_version() {
	local new_version="$1"

	# The current on-disk workspace version. Both call sites run against a fresh
	# checkout of BASE_SHA, so this is always main's X.Y.0-nightly.
	local current_version
	current_version=$(cargo metadata --format-version 1 --no-deps | \
		jq -r '.packages | map(select(.name == "surrealdb"))[0].version')
	if [[ -z "$current_version" || "$current_version" == "null" ]]; then
		echo "Error: could not determine the current workspace version"
		exit 1
	fi

	# Rewrite the workspace version wherever it appears in the root manifest:
	# [workspace.package].version and every surrealdb* entry in
	# [workspace.dependencies] carry it verbatim, while the independently
	# versioned crates (surrealism*, surrealml-*) use their own distinct version
	# strings and are left untouched. A direct rewrite is used because it is
	# direction-agnostic: a minor cut moves nightly -> beta, which is a semver
	# "downgrade" (nightly sorts above beta) that version-bumping tools refuse to
	# perform.
	if [[ "$current_version" != "$new_version" ]]; then
		perl -pi -e "s/\"\Q${current_version}\E\"/\"${new_version}\"/g" Cargo.toml
		if grep -q "\"${current_version}\"" Cargo.toml; then
			echo "Error: '${current_version}' still present in Cargo.toml after rewrite"
			exit 1
		fi
	fi

	# Sync the lockfile to the rewritten versions.
	cargo update -p surrealdb -p surrealdb-core -p surrealdb-server
}

# ----------------------------------------------------------------------------
# Create the release branch at its first pre-release version (beta.1 for a minor
# cut, alpha.1 for a major cut).
# ----------------------------------------------------------------------------
if [[ -n "$RELEASE_BRANCH" ]]; then
	reuse_existing=false
	if git ls-remote --exit-code --heads origin "${RELEASE_BRANCH}" >/dev/null 2>&1; then
		# The branch already exists. Reuse it ONLY if it is exactly what this cut
		# would create (i.e. a prior run of THIS cut that failed after pushing the
		# branch but before finishing), so a retry is safe. Otherwise refuse, so a
		# real, pre-existing release line is never silently reused or clobbered.
		# Match on both the base commit it was cut from and the pre-release
		# version it carries: a genuine older line sits at a later version (e.g.
		# 3.2.5, not 3.2.0-beta.1) and would correctly fail this check.
		git fetch --no-tags origin "${RELEASE_BRANCH}" >/dev/null 2>&1
		existing_parent="$(git rev-parse 'FETCH_HEAD^' 2>/dev/null || echo '')"
		existing_version="$(git show 'FETCH_HEAD:Cargo.toml' 2>/dev/null | \
			awk '/^\[workspace.package\]/{f=1} f&&/^version[[:space:]]*=/{gsub(/version[[:space:]]*=[[:space:]]*"|"/,""); print; exit}')"
		if [[ "$existing_parent" == "$BASE_SHA" && "$existing_version" == "$BRANCH_VERSION" ]]; then
			echo "${RELEASE_BRANCH} already exists at ${BRANCH_VERSION} cut from ${BASE_SHA} (a prior run of this cut); reusing it and continuing to the main bump."
			reuse_existing=true
		else
			echo "::error::${RELEASE_BRANCH} already exists on the remote and does not match this cut (parent '${existing_parent}' vs '${BASE_SHA}', version '${existing_version}' vs '${BRANCH_VERSION}'); refusing to overwrite it."
			exit 1
		fi
	fi

	if [[ "$reuse_existing" != "true" ]]; then
		git checkout -b "${RELEASE_BRANCH}" "${BASE_SHA}"
		set_workspace_version "${BRANCH_VERSION}"
		git commit -am "Set version to ${BRANCH_VERSION}"

		if [[ "$PUBLISH" == "true" ]]; then
			git push origin "${RELEASE_BRANCH}"
			echo "Pushed ${RELEASE_BRANCH} at ${BRANCH_VERSION} (rolling build will produce its artifacts)"
		else
			echo "[Dry-run] Would push ${RELEASE_BRANCH} at ${BRANCH_VERSION}"
		fi
	fi
fi

# ----------------------------------------------------------------------------
# Open the PR to move main to its next development version
# ----------------------------------------------------------------------------
git checkout -B "main-bump" "${BASE_SHA}"
set_workspace_version "${MAIN_VERSION}"

if git diff --quiet; then
	echo "main is already on ${MAIN_VERSION}; nothing to bump"
	exit 0
fi
git commit -am "Bump version to ${MAIN_VERSION}"

PR_BRANCH="dev/ci/v${MAIN_VERSION}"

git branch -M "main-bump" "${PR_BRANCH}"

if [[ "$PUBLISH" != "true" ]]; then
	echo "[Dry-run] Would create PR to bump main to ${MAIN_VERSION}"
	exit 0
fi

# Update the remote PR branch IN PLACE rather than deleting + recreating it, so
# an existing PR keeps its identity and review state across retries.
# --force-with-lease, leased to the tip we just observed, makes the overwrite
# safe (it refuses if the branch moved since); a missing branch is a plain
# create. Kept below the publish gate so a dry-run never mutates the remote.
if remote_sha="$(git ls-remote --exit-code origin "refs/heads/${PR_BRANCH}" 2>/dev/null | cut -f1)"; then
	git push --force-with-lease="${PR_BRANCH}:${remote_sha}" origin "HEAD:${PR_BRANCH}"
else
	git push origin "HEAD:${PR_BRANCH}"
fi

PR_TITLE="Bump version to ${MAIN_VERSION}"
PR_BODY="Automated main version bump following a ${CUT_TYPE} cut.

**This PR moves the main branch to its next development version.**

- Cut type: \`${CUT_TYPE}\`
- Main branch version: \`${MAIN_VERSION}\`"
if [[ -n "$RELEASE_BRANCH" ]]; then
	PR_BODY="${PR_BODY}
- New release branch: \`${RELEASE_BRANCH}\` (\`${BRANCH_VERSION}\`)"
fi
PR_BODY="${PR_BODY}

Review and merge this PR to prepare main for the next development cycle."

existing_pr=$(gh pr list --head "${PR_BRANCH}" --base main --json number -q '.[0].number' 2>/dev/null || echo "")
if [[ -n "$existing_pr" ]]; then
	echo "PR #${existing_pr} already exists, updating it"
	gh pr edit "${existing_pr}" --title "${PR_TITLE}" --body "${PR_BODY}"
else
	gh pr create --base main --head "${PR_BRANCH}" --title "${PR_TITLE}" --body "${PR_BODY}"
	echo "Created PR to bump main to ${MAIN_VERSION}"
fi
