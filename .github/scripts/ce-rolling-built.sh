#!/usr/bin/env bash
# Decides whether this ce-rolling-build.yml run may write its commit's set on
# its branch. Prints `true` when the set is already stored, so the run writes
# nothing and succeeds on the stored set; prints `false` when this run is the
# one to write it; fails when a newer run of the commit on the branch exists.
#
# The set is stored once some run of the commit on the branch has succeeded:
# another run, or an earlier attempt of this one ("Re-run all jobs" keeps the
# run id). Until then only the newest run of the commit on the branch writes
# it. An older run's re-run would otherwise complete a set on top of objects a
# newer run had already overwritten, and store a mix of two compiles. The
# newest run writes every object of the set itself, and its own re-runs reuse
# its own build, so the set a successful run leaves was written by that run.
# ce-rolling-build.yml checks this before compiling and again before each
# write, because a re-run of failed jobs does not re-run prepare.
#
# Reads the GitHub Actions environment (GITHUB_API_URL, GITHUB_REPOSITORY,
# GITHUB_SHA, GITHUB_REF_NAME, GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT) and GH_TOKEN,
# which needs actions: read. Fails on any API error rather than guess.
set -euo pipefail

api() {
	curl -fsS --retry 3 --retry-delay 2 \
		-H "Authorization: Bearer ${GH_TOKEN}" \
		-H "Accept: application/vnd.github+json" \
		"${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/$1"
}

branch="$(jq -rn --arg b "${GITHUB_REF_NAME}" '$b | @uri')"
runs="actions/workflows/ce-rolling-build.yml/runs?head_sha=${GITHUB_SHA}&branch=${branch}"

others=$(api "${runs}&status=success&per_page=100" |
	jq --arg b "${GITHUB_REF_NAME}" --argjson id "${GITHUB_RUN_ID}" \
		'[.workflow_runs[] | select(.head_branch == $b and .id != $id)] | length')
if ((others > 0)); then
	echo true
	exit 0
fi

for ((attempt = 1; attempt < GITHUB_RUN_ATTEMPT; attempt++)); do
	if [[ "$(api "actions/runs/${GITHUB_RUN_ID}/attempts/${attempt}" | jq -r '.conclusion')" == success ]]; then
		echo true
		exit 0
	fi
done

# The newest run comes first, and this run is always listed.
newest=$(api "${runs}&per_page=1" |
	jq -r --arg b "${GITHUB_REF_NAME}" \
		'.workflow_runs[0] | if .head_branch == $b then "\(.id)\t\(.html_url)" else error("listed a run of another branch") end')
IFS=$'\t' read -r id url <<<"${newest}"
if [[ "${id}" != "${GITHUB_RUN_ID}" ]]; then
	echo "::error title=Superseded::A newer rolling build of ${GITHUB_SHA} on ${GITHUB_REF_NAME} exists, and only the newest run of a commit writes its set. Re-run that one instead: ${url}" >&2
	exit 1
fi
echo false
