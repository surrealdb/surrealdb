#!/usr/bin/env bash
set -euo pipefail

# Advance main to the next development version after a stable .0 ships.
#
# Releasing a stable `X.Y.0` means the X.Y line has gone stable. If main is still
# the in-flight development version for that exact line (`X.Y.0-nightly`), move it
# on to the next minor's development version (`X.(Y+1).0-nightly`) via a PR.
#
# This is what carries main forward after a *major* cut: a major cut creates
# `releases/X.0` at `X.0.0-alpha.1` and parks main at `X.0.0-nightly`; main only
# advances to `X.1.0-nightly` once `X.0.0` actually ships stable.
#
# It is a deliberate no-op for:
#   * pre-releases (`X.Y.0-alpha/beta/rc`) and patches (`X.Y.Z`, Z>0) - the .0 of
#     the line has not shipped yet, or the line is already past its .0;
#   * minor lines, whose cut already advanced main past `X.Y.0-nightly` (the guard
#     below then finds main on a later version and does nothing).
#
# Usage: advance-main-after-release.sh <released-version> [publish]
#   publish=false performs a dry-run (no push, no PR).

RELEASED_VERSION="${1:-}"
PUBLISH="${2:-false}"

if [[ -z "$RELEASED_VERSION" ]]; then
	echo "Error: released version argument required"
	echo "Usage: $0 <released-version> [publish]"
	exit 1
fi

# Only a stable X.Y.0 can advance main. Pre-releases and patch releases are no-ops.
if [[ ! "$RELEASED_VERSION" =~ ^([0-9]+)\.([0-9]+)\.0$ ]]; then
	echo "Released version ${RELEASED_VERSION} is not a stable X.Y.0; main is not advanced."
	exit 0
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"

# Configure git identity for the commit (fresh runners have none)
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

git fetch origin main
git checkout main
git pull origin main

MAIN_VERSION=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages | map(select(.name == "surrealdb"))[0].version')

if [[ -z "$MAIN_VERSION" || "$MAIN_VERSION" == "null" ]]; then
	echo "Error: could not determine the main version from the code"
	exit 1
fi

# Guard: only advance when main is still the in-flight version for *this* line.
# A minor line will already have moved main past X.Y.0-nightly at cut time.
EXPECTED="${MAJOR}.${MINOR}.0-nightly"
if [[ "$MAIN_VERSION" != "$EXPECTED" ]]; then
	echo "main is on ${MAIN_VERSION}, not ${EXPECTED}; it has already advanced past the ${MAJOR}.${MINOR} line. Nothing to do."
	exit 0
fi

NEXT_VERSION="${MAJOR}.$((MINOR + 1)).0-nightly"
echo "Advancing main from ${MAIN_VERSION} to ${NEXT_VERSION} after the ${RELEASED_VERSION} stable release"

# Dynamically build the list of surrealdb-* packages (excludes surrealism-*).
PACKAGES=$(cargo metadata --format-version 1 --no-deps | \
	jq -r '.packages[].name' | \
	grep '^surrealdb' | \
	sed 's/^/--package /' | \
	tr '\n' ' ')

# shellcheck disable=SC2086 # PACKAGES is an intentional list of --package args
cargo set-version $PACKAGES "${NEXT_VERSION}"
cargo update -p surrealdb -p surrealdb-core -p surrealdb-server

if git diff --quiet; then
	echo "main is already on ${NEXT_VERSION}; nothing to bump"
	exit 0
fi
git commit -am "Bump version to ${NEXT_VERSION}"

PR_BRANCH="dev/ci/v${NEXT_VERSION}"
git checkout -B "${PR_BRANCH}"

if [[ "$PUBLISH" != "true" ]]; then
	echo "[Dry-run] Would create PR to advance main to ${NEXT_VERSION}"
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

PR_TITLE="Bump version to ${NEXT_VERSION}"
PR_BODY="Automated main version bump following the \`${RELEASED_VERSION}\` stable release.

**This PR moves main onto the next development version now that \`${MAJOR}.${MINOR}.0\` has shipped.**

- Released version: \`${RELEASED_VERSION}\`
- Main branch version: \`${NEXT_VERSION}\`

Review and merge this PR to prepare main for the next development cycle."

existing_pr=$(gh pr list --head "${PR_BRANCH}" --base main --json number -q '.[0].number' 2>/dev/null || echo "")
if [[ -n "$existing_pr" ]]; then
	echo "PR #${existing_pr} already exists, updating it"
	gh pr edit "${existing_pr}" --title "${PR_TITLE}" --body "${PR_BODY}"
	PR_URL=$(gh pr view "${existing_pr}" --json url -q '.url')
else
	PR_URL=$(gh pr create --base main --head "${PR_BRANCH}" --title "${PR_TITLE}" --body "${PR_BODY}")
	echo "Created PR to advance main to ${NEXT_VERSION}"
fi

echo "PR: ${PR_URL}"

# Surface the PR URL as a step output when running in GitHub Actions so the
# release summary can list it.
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	echo "pr-url=${PR_URL}" >> "$GITHUB_OUTPUT"
fi
