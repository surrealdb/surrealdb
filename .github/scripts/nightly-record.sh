#!/usr/bin/env bash
# Keeps the nightly channel from moving back. Every nightly on a line is the
# same version, so only its commit tells an older nightly from a newer one.
# s3://<BUCKET>/releases/nightly/release.json names (.sha) the commit of the
# last nightly promoted. Both release workflows write it in their promote job,
# which takes turns with every other release's in the publish-pointers lane,
# before anything public is written: ce-release.yml with `write`, release.yml
# through its promote-binaries action.
#
#   check    Fails unless GIT_SHA is the recorded commit or ahead of it; with
#            no record, passes. Promote runs it before the record is written,
#            so an older nightly publishes nothing.
#   write    Records GIT_SHA, with VERSION and this run.
#   current  Prints `true` when the record names GIT_SHA and `false` when it
#            names another commit. A nightly's mirror moves the nightly tags
#            only while it is current: once a newer nightly has promoted, that
#            nightly's own mirror moves them. A missing record fails, since
#            promote wrote one.
#
# Reads BUCKET, GIT_SHA, VERSION (write), GITHUB_API_URL, GITHUB_REPOSITORY,
# GITHUB_SERVER_URL, GITHUB_RUN_ID, GH_TOKEN (check: contents read) and
# RUNNER_TEMP, with ambient AWS credentials that can read and write the
# bucket. Only a missing record counts as none; any other failure exits 1.
set -euo pipefail

: "${BUCKET:?}" "${GIT_SHA:?}" "${RUNNER_TEMP:?}"
key="releases/nightly/release.json"

# The recorded commit, or nothing when there is no record.
recorded() {
	local err
	if err=$(aws s3api get-object --bucket "${BUCKET}" --key "${key}" "${RUNNER_TEMP}/nightly-record.json" 2>&1 >/dev/null); then
		if ! jq -er '.sha | strings' "${RUNNER_TEMP}/nightly-record.json"; then
			echo "::error::s3://${BUCKET}/${key} names no commit (.sha)." >&2
			return 1
		fi
	elif grep -q 'NoSuchKey' <<<"${err}"; then
		:
	else
		echo "::error::Could not read s3://${BUCKET}/${key} (${err}). Re-run this job." >&2
		return 1
	fi
}

case "${1:-}" in
check)
	sha="$(recorded)"
	if [[ -n "${sha}" && "${sha}" != "${GIT_SHA}" ]]; then
		if ! status=$(curl -fsS --retry 3 --retry-delay 2 \
			-H "Authorization: Bearer ${GH_TOKEN}" \
			-H "Accept: application/vnd.github+json" \
			"${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/compare/${sha}...${GIT_SHA}?per_page=1" | jq -er '.status'); then
			echo "::error::Could not compare ${GIT_SHA} with ${sha}, the commit of the published nightly."
			exit 1
		fi
		if [[ "${status}" != ahead && "${status}" != identical ]]; then
			echo "::error title=Not newer than the published nightly::The published nightly is from ${sha}, and ${GIT_SHA} is not ahead of it (${status}), so releasing it would move the nightly back."
			exit 1
		fi
	fi
	echo "Releasing the nightly from ${GIT_SHA} (published: ${sha:-none})."
	;;
write)
	: "${VERSION:?}"
	jq -n --arg sha "${GIT_SHA}" --arg version "${VERSION}" \
		--arg run "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}" \
		--arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		'{sha: $sha, release: {name: "nightly", version: $version, released_at: $at}, run: $run}' |
		aws s3 cp - "s3://${BUCKET}/${key}"
	;;
current)
	sha="$(recorded)"
	if [[ -z "${sha}" ]]; then
		echo "::error::s3://${BUCKET}/${key} is missing, though promote wrote it. Re-run this job." >&2
		exit 1
	fi
	if [[ "${sha}" == "${GIT_SHA}" ]]; then
		echo true
	else
		echo "::notice::A newer nightly (${sha}) has been promoted since; its mirror moves the nightly tags." >&2
		echo false
	fi
	;;
*)
	echo "usage: nightly-record.sh check | write | current" >&2
	exit 1
	;;
esac
