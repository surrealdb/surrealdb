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
# The independently-versioned families (surrealism, surrealml) travel with the
# engine on their own 0.x lines. Each cut moves them in the same shape as
# surrealdb: the release line takes their current nightly minor as its
# pre-release (0.m.0-nightly -> 0.m.0-beta.1), and main advances one minor
# (0.m.0-nightly -> 0.(m+1).0-nightly). A cut thus reserves one of each
# family's minors per release line, keeping their numbers independent of
# surrealdb's while managed by the same rule. (advance-main-after-release.sh
# leaves them alone - they already advanced here, at cut time.)
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

crate_current_version() {
	cargo metadata --format-version 1 --no-deps \
		| jq -r --arg n "$1" '.packages | map(select(.name == $n))[0].version'
}

# Every workspace member manifest, including the root (which carries
# [workspace.package] and [workspace.dependencies]). The rewrite spans all of
# them: surrealdb crates inherit their version from [workspace.package] so only
# the root manifest names it, but the independent families declare an explicit
# version in each crate's own manifest, so those must be rewritten too.
mapfile -t MANIFESTS < <(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].manifest_path')

# Direction-agnostic rewrite of one version string to another across every
# manifest, verifying the old string is gone. Direct string replacement rather
# than a bump tool because a minor cut moves nightly -> beta, which is a semver
# "downgrade" (nightly sorts above beta) that bump tools refuse to perform. The
# strings are specific pre-release versions unique to a single family, so a
# broad replace never touches an unrelated dependency.
rewrite_version_string() {
	local from="$1" to="$2"
	[[ "$from" == "$to" ]] && return 0
	perl -pi -e "s/\"\Q${from}\E\"/\"${to}\"/g" "${MANIFESTS[@]}"
	if grep -qF -- "\"${from}\"" "${MANIFESTS[@]}"; then
		echo "::error::'${from}' still present in a workspace manifest after rewrite"
		exit 1
	fi
}

# Rewrite every family's version for one leg of the cut. mode is "branch" (the
# release line's first pre-release) or "main" (the next development version).
# Both call sites run against a fresh checkout of BASE_SHA, so every family is
# on its X.Y.0-nightly development version on entry.
set_workspace_version() {
	local mode="$1" surrealdb_target="$2"

	# surrealdb: [workspace.package].version + every surrealdb* dependency entry
	# carry the version verbatim; the caller already computed the exact target.
	local cur_sdb
	cur_sdb=$(crate_current_version surrealdb)
	if [[ -z "$cur_sdb" || "$cur_sdb" == "null" ]]; then
		echo "Error: could not determine the current workspace version"
		exit 1
	fi

	# The independently-versioned families are every published workspace member
	# that does not carry surrealdb's version: the surrealdb crates inherit it
	# from [workspace.package], so anything published on a different string is on
	# its own 0.x line. One representative crate per family is enough — a family's
	# crates share a single version string, so rewrite_version_string moves them
	# all together (via [workspace.dependencies] and each crate's own [package]),
	# and naming one member for `cargo update -p` reconciles the rest. Derived
	# from metadata rather than a hand-maintained list so a newly added family is
	# picked up automatically, and captured before the surrealdb rewrite below so
	# the "version != cur_sdb" filter still sees surrealdb on its pre-cut string.
	# publish=false members (demo, test-only crates) are excluded: the discipline
	# only governs published lines.
	local -a independent_anchors
	mapfile -t independent_anchors < <(
		cargo metadata --format-version 1 --no-deps \
			| jq -r --arg sdb "$cur_sdb" \
				'[ .packages[] | select(.publish != []) | select(.version != $sdb) ]
				 | group_by(.version) | map(.[0].name) | .[]'
	)

	rewrite_version_string "$cur_sdb" "$surrealdb_target"

	# Each family travels with the engine on its own 0.x line: the release line
	# takes the SAME nightly->pre-release transform on its current 0.m minor
	# (0.m.0-nightly -> 0.m.0-<beta.1|alpha.1>), and main advances one minor
	# (0.m.0-nightly -> 0.(m+1).0-nightly). Each release line therefore reserves
	# one of each family's minors, mirroring surrealdb's per-line cadence without
	# tying the family's number to surrealdb's. The pre-release suffix matches
	# surrealdb's (from BRANCH_VERSION).
	local suffix="${BRANCH_VERSION#*-}"
	local anchor cur target
	for anchor in "${independent_anchors[@]}"; do
		cur=$(crate_current_version "$anchor")
		if [[ ! "$cur" =~ ^([0-9]+)\.([0-9]+)\.0-nightly$ ]]; then
			echo "::error::${anchor} is on '${cur}', expected M.m.0-nightly; refusing to cut from an inconsistent independent-crate version."
			exit 1
		fi
		if [[ "$mode" == "branch" ]]; then
			target="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.0-${suffix}"
		else
			target="${BASH_REMATCH[1]}.$((BASH_REMATCH[2] + 1)).0-nightly"
		fi
		rewrite_version_string "$cur" "$target"
	done

	# Regenerate the lockfile from the rewritten manifests WITHOUT upgrading any
	# external dependency: name only workspace members, so cargo reconciles their
	# entries and leaves everything else pinned. `cargo update` with no package
	# specs (or --workspace with unpinned deps) would pull in unrelated updates,
	# which risks a broken tree and belongs in its own deliberate PR, not a cut.
	# The surrealdb representatives cover the surrealdb crates; each family anchor
	# covers its family.
	local -a update_specs=(-p surrealdb -p surrealdb-core -p surrealdb-server)
	for anchor in "${independent_anchors[@]}"; do
		update_specs+=(-p "$anchor")
	done
	cargo update "${update_specs[@]}"
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
		set_workspace_version branch "${BRANCH_VERSION}"
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
set_workspace_version main "${MAIN_VERSION}"

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
