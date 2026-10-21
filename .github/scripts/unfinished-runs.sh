#!/usr/bin/env bash
# Prints the unfinished runs of a workflow that are not dry-runs ("[dry-run]"
# title), one "<id>\t<title>\t<url>" line each.
#
# Only unfinished runs are asked for, by status and every page of each, so a
# run is found however many runs were created after it. The statuses are read
# in the order a run moves through them, so a run that moves on while this
# reads is found at its later status. A run steps back from in_progress to
# waiting when it reaches an environment gate, and on to in_progress again once
# approved, so those two are read a second time: a run that reaches the gate
# and is approved while this reads is found at one of them. For a workflow
# whose runs pass at most one gate, as the release workflows' runs pass only
# the approval gate, every run that started before this began and has not
# finished when it ends is listed.
#
# A workflow the Actions API does not know (a 404 on the first request, as for
# a workflow file not yet on the default branch) has no runs. Any other failure
# prints nothing and exits 1.
#
# Usage: unfinished-runs.sh <workflow-file>
#
# Reads GITHUB_API_URL, GITHUB_REPOSITORY and GH_TOKEN (actions: read).
set -euo pipefail

workflow="${1:?usage: unfinished-runs.sh <workflow-file>}"

body="$(mktemp)"
runs="$(mktemp)"
trap 'rm -f "${body}" "${runs}"' EXIT

first=true
for status in requested action_required pending queued waiting in_progress waiting in_progress; do
	page=1
	while :; do
		code="$(curl -sS --retry 3 --retry-delay 2 -o "${body}" -w '%{http_code}' \
			-H "Authorization: Bearer ${GH_TOKEN}" \
			-H "Accept: application/vnd.github+json" \
			"${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/actions/workflows/${workflow}/runs?status=${status}&per_page=100&page=${page}")" || code=000
		case "${code}" in
		200) ;;
		404) if [[ "${first}" == true ]]; then exit 0; fi ;&
		*)
			echo "::error::Could not list the ${workflow} runs (HTTP ${code})." >&2
			exit 1
			;;
		esac
		if ! count="$(jq -e '.workflow_runs | arrays | length' "${body}")"; then
			echo "::error::Could not read the ${workflow} runs." >&2
			exit 1
		fi
		first=false
		jq -r '.workflow_runs[] | select(.display_title | startswith("[dry-run]") | not)
			| "\(.id)\t\(.display_title)\t\(.html_url)"' "${body}" >>"${runs}"
		((count == 100)) || break
		if ((page == 20)); then
			echo "::error::${workflow} has more ${status} runs than this reads." >&2
			exit 1
		fi
		page=$((page + 1))
	done
done

# A run that moved on while this read is listed at each status it was seen at.
sort -u "${runs}"
