#!/usr/bin/env bash
# Fails when another run of a release workflow may be publishing the given
# version.
#
# release.yml and ce-release.yml release the same versions, and neither's
# concurrency serializes two releases of one version (ce-release.yml releases
# lines in parallel; release.yml keys on its git-sha input). Each calls this
# from its approval job, after approval and before the version is claimed,
# once against itself and once against the other, so two runs never both find
# the version unclaimed (claim-release-version.sh). Releases of different
# versions run side by side: the jobs that move pointers they share take turns
# in the publish-* concurrency lanes instead.
#
# A run may be publishing from the moment its approval job ("Approve release
# <name> (...") starts running until the run finishes. Every attempt's jobs
# are read, so an approval that succeeded in an earlier attempt still counts
# while a retry runs; a run re-run in full also counts while its new attempt
# waits at the gate again. A run that is still preparing, is parked at the
# approval gate, or is a dry-run ("[dry-run]" title) is not publishing: it
# makes this same check once it is approved. Two runs approved at the same
# moment each see the other's approval job running, so both stop rather than
# both publish. The caller's own run never counts.
#
# A release.yml run that releases no engine claims nothing and never counts.
# The release.yml of a releases/3.x line, which still carries a `components`
# input, titles such a run "... components: internal)", "... components:
# test)" or "Nightly release (internal)"; a title in any other format counts.
#
# The unfinished runs come from unfinished-runs.sh, which finds a run however
# many came after it, and gives a workflow the Actions API does not know no
# runs.
#
# Usage: release-publishing.sh <workflow-file> <release-name>
#
# Reads GITHUB_API_URL, GITHUB_REPOSITORY, GITHUB_RUN_ID and GH_TOKEN (actions:
# read). Exits 0 when no run may be publishing the version, and 1 when one may
# be (listed) or the runs cannot be read.
set -euo pipefail

workflow="${1:?usage: release-publishing.sh <workflow-file> <release-name>}"
name="${2:?usage: release-publishing.sh <workflow-file> <release-name>}"
approval="Approve release ${name} ("
what="${workflow} run releasing ${name}"

get() {
	curl -sS --retry 3 --retry-delay 2 -o "$2" -w '%{http_code}' \
		-H "Authorization: Bearer ${GH_TOKEN}" \
		-H "Accept: application/vnd.github+json" \
		"${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/$1"
}

ignore=""
if [[ "${workflow}" == release.yml ]]; then
	ignore='^Nightly release \(internal\)$|, components: (internal|test)\)$'
fi

body="$(mktemp)"
jobs="$(mktemp)"
trap 'rm -f "${body}" "${jobs}"' EXIT

# Assigned before the loop, so a failed listing stops here rather than reading
# as no runs.
if ! candidates="$(bash "$(dirname "$0")/unfinished-runs.sh" "${workflow}")"; then
	echo "::error::Could not list the ${workflow} runs, so whether one is publishing is unknown. Re-run this job."
	exit 1
fi

publishing=()
while IFS=$'\t' read -r id title url; do
	[[ -n "${id}" && "${id}" != "${GITHUB_RUN_ID:-0}" ]] || continue
	if [[ -n "${ignore}" && "${title}" =~ ${ignore} ]]; then
		continue
	fi
	: >"${jobs}"
	page=1
	while :; do
		code="$(get "actions/runs/${id}/jobs?filter=all&per_page=100&page=${page}" "${body}")" || code=000
		if [[ "${code}" != 200 ]] || ! count="$(jq -e '.jobs | arrays | length' "${body}")"; then
			echo "::error::Could not read the jobs of ${url} (HTTP ${code}), so whether it is publishing is unknown. Re-run this job."
			exit 1
		fi
		cat "${body}" >>"${jobs}"
		((count == 100)) || break
		if ((page == 20)); then
			echo "::error::${url} has more jobs than this reads, so whether it is publishing is unknown."
			exit 1
		fi
		page=$((page + 1))
	done
	set +e
	jq -e -s --arg approval "${approval}" '[.[].jobs[] | select((.name | startswith($approval))
		and (.status == "in_progress" or (.status == "completed" and .conclusion == "success")))]
		| length > 0' "${jobs}" >/dev/null
	rc=$?
	set -e
	case "${rc}" in
	0) publishing+=("${title}"$'\t'"${url}") ;;
	1) ;;
	*)
		echo "::error::Could not read the jobs of ${url}, so whether it is publishing is unknown. Re-run this job."
		exit 1
		;;
	esac
done <<<"${candidates}"

if ((${#publishing[@]} > 0)); then
	echo "::error title=${workflow} is publishing::An approved ${what} is unfinished and may be publishing. Let it finish or cancel it, then re-run this job:"
	printf '  %s\n' "${publishing[@]}"
	exit 1
fi
echo "No approved ${what} is unfinished."
