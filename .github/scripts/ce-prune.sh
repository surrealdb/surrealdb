#!/usr/bin/env bash
# Deletes the community rolling sets that are old and that no release can
# still need.
#
# A set is one commit's build on one branch: s3://<bucket>/ce-builds/<slug>/<sha>/
# and the ECR tags ce-<slug>-<sha> and ce-<slug>-<sha>-dev (see
# ce-rolling-set.sh). A set is deleted whole, and only when:
#
#   * its newest object and image were stored more than MAX_AGE_DAYS ago;
#   * it is not the newest successful rolling build of main or of a releases/*
#     branch. The rolling build stores a commit's set only once, so a deleted
#     set cannot be rebuilt, and a line that has not moved in MAX_AGE_DAYS must
#     stay releasable;
#   * its commit is not bound to a version with an attempt, overwrite
#     included, that has not completed: releases/<name>/engine.sha with no
#     releases/<name>/engine.json, or written after it (every attempt rewrites
#     the binding and a completed one then the manifest; see
#     claim-release-version.sh). Such an attempt is finished by re-running it,
#     which reads the set.
#
# Nothing is deleted while a ce-release.yml run that is not a dry-run is
# unfinished, because it may yet read its set. From the other side, the
# release's approval job refuses to publish while a cleanup is unfinished, and
# otherwise checks that its set still matches what prepare verified. A release
# is unfinished from the moment it starts, so any cleanup either sees it and
# deletes nothing, or started first and is found by that check still running
# or already done.
#
# Nothing outside ce-builds/ and the ce-<slug>-<sha>[-dev] tags is touched.
#
# Reads BUCKET, ECR_REPOSITORY, ECR_REGION, MAX_AGE_DAYS, DRY_RUN ('true'
# lists what would go and deletes nothing), GITHUB_API_URL, GITHUB_REPOSITORY
# and GH_TOKEN (actions: read, contents: read), plus ambient AWS credentials
# that can list, read and delete in the bucket and the ECR repository. Any
# failed read stops it before it deletes anything; a failed delete is reported,
# the rest continue, and it exits 1.
set -euo pipefail

# The AWS CLI prints ECR's times in the local zone and S3's in UTC; in UTC
# both read as UTC, which the cutoff is.
export TZ=UTC

: "${BUCKET:?}" "${ECR_REPOSITORY:?}" "${ECR_REGION:?}" "${MAX_AGE_DAYS:?}"
if ! [[ "${MAX_AGE_DAYS}" =~ ^[1-9][0-9]*$ ]]; then
	echo "::error::MAX_AGE_DAYS must be a whole number of days, at least 1; got '${MAX_AGE_DAYS}'."
	exit 1
fi
cutoff="$(date -u -d "${MAX_AGE_DAYS} days ago" +%Y-%m-%dT%H:%M:%S)"

api() {
	curl -fsS --retry 3 --retry-delay 2 \
		-H "Authorization: Bearer ${GH_TOKEN}" \
		-H "Accept: application/vnd.github+json" \
		"${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/$1"
}
slug() { bash "$(dirname "$0")/ce-rolling-set.sh" slug "$1"; }
is_sha() { [[ "$1" =~ ^[0-9a-f]{40}$ ]]; }

# Every unfinished ce-release.yml run that is not a dry-run, however many runs
# came after it.
if ! listed="$(bash "$(dirname "$0")/unfinished-runs.sh" ce-release.yml)"; then
	echo "::error::Could not list the ce-release.yml runs, so whether one is unfinished is unknown."
	exit 1
fi
unfinished=()
while IFS=$'\t' read -r _ _ url; do
	[[ -n "${url}" ]] && unfinished+=("${url}")
done <<<"${listed}"
if ((${#unfinished[@]} > 0)); then
	echo "::warning title=Cleanup skipped::A release is unfinished and may yet read its rolling set, so nothing is deleted this time:"
	printf '  %s\n' "${unfinished[@]}"
	exit 0
fi

# "<slug>/<sha>" of the newest successful rolling build of main and of every
# releases/* branch.
if ! refs="$(api "git/matching-refs/heads/releases/")"; then
	echo "::error::Could not list the release branches."
	exit 1
fi
branches="$(printf 'main\n'; jq -r '.[].ref | ltrimstr("refs/heads/")' <<<"${refs}")"
kept=""
while IFS= read -r branch; do
	[[ -n "${branch}" ]] || continue
	if ! runs="$(api "actions/workflows/ce-rolling-build.yml/runs?branch=$(jq -rn --arg b "${branch}" '$b | @uri')&status=success&per_page=1")"; then
		echo "::error::Could not read the rolling builds of ${branch}."
		exit 1
	fi
	sha="$(jq -r 'if (.workflow_runs | type) == "array" then .workflow_runs[0].head_sha // "" else error("no workflow_runs") end' <<<"${runs}")"
	[[ -n "${sha}" ]] && kept+=" $(slug "${branch}")/${sha}"
done <<<"${branches}"

# The commits bound to a version with an attempt that has not completed. Both
# times come from the same listing, in one format, so they compare as text.
keys="$(aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix releases/ \
	--query "Contents[?ends_with(Key, '/engine.sha') || ends_with(Key, '/engine.json')].[Key, LastModified]" \
	--output text)"
pending="$(awk -F'\t' '{ n = split($1, p, "/") }
	n == 3 && p[1] == "releases" { if (p[3] == "engine.sha") bound[p[2]] = $2; else if (p[3] == "engine.json") done[p[2]] = $2 }
	END { for (v in bound) if (!(v in done) || bound[v] >= done[v]) print v }' <<<"${keys}")"
held=""
while IFS= read -r name; do
	[[ -n "${name}" ]] || continue
	sha="$(aws s3 cp --quiet "s3://${BUCKET}/releases/${name}/engine.sha" - | tr -d '[:space:]')"
	if ! is_sha "${sha}"; then
		echo "::error::releases/${name}/engine.sha does not hold a commit ('${sha}'); deleting nothing."
		exit 1
	fi
	held+=" ${sha}"
done <<<"${pending}"

# One "<slug> <sha> <time>" record per stored object and per image, with times
# cut to the second, which drops the offset: both are UTC (TZ above), so S3's
# and ECR's times compare as text.
s3="$(aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix ce-builds/ \
	--query 'Contents[].[Key, LastModified]' --output text)"
ecr="$(aws ecr describe-images --region "${ECR_REGION}" --repository-name "${ECR_REPOSITORY}" \
	--filter tagStatus=TAGGED --query "imageDetails[].[imagePushedAt, join(' ', imageTags)]" \
	--output text)"
# No interval expressions ({40}): mawk, Ubuntu's awk, does not reliably have
# them.
records="$(
	awk -F'\t' 'function is_sha(x) { return length(x) == 40 && x !~ /[^0-9a-f]/ }
		{ n = split($1, p, "/"); if (n >= 4 && is_sha(p[3])) print p[2], p[3], substr($2, 1, 19) }' <<<"${s3}"
	awk -F'\t' 'function is_sha(x) { return length(x) == 40 && x !~ /[^0-9a-f]/ }
		{ n = split($2, tags, " ")
		for (i = 1; i <= n; i++) {
			t = tags[i]; sub(/-dev$/, "", t)
			sha = substr(t, length(t) - 39)
			if (substr(t, 1, 3) != "ce-" || length(t) < 45 || substr(t, length(t) - 40, 1) != "-" || !is_sha(sha)) continue
			print substr(t, 4, length(t) - 44), sha, substr($1, 1, 19)
		} }' <<<"${ecr}"
)"

# A time that is not an ISO date (an AWS CLI set to print epoch seconds) cannot
# be compared with the cutoff, so nothing is deleted on a guess.
if ! bad="$(awk 'NF == 3 && $3 !~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/ { print; exit 1 }' <<<"${records}")"; then
	echo "::error::Unexpected timestamp format from the AWS CLI (${bad}); deleting nothing."
	exit 1
fi

doomed="$(awk -v cutoff="${cutoff}" -v kept="${kept}" -v held="${held}" '
	BEGIN {
		n = split(kept, k, " "); for (i = 1; i <= n; i++) keep[k[i]] = 1
		n = split(held, h, " "); for (i = 1; i <= n; i++) hold[h[i]] = 1
	}
	NF == 3 { set = $1 "/" $2; if ($3 > age[set]) age[set] = $3 }
	END {
		for (set in age) {
			split(set, s, "/")
			if (age[set] >= cutoff || (set in keep) || (s[2] in hold)) continue
			print s[1], s[2], age[set]
		}
	}' <<<"${records}" | sort)"

if [[ -z "${doomed}" ]]; then
	echo "No rolling set can be deleted: none older than ${MAX_AGE_DAYS} days (before ${cutoff}Z) is unneeded."
	exit 0
fi

echo "Rolling sets older than ${MAX_AGE_DAYS} days (before ${cutoff}Z) that no release needs: $(wc -l <<<"${doomed}" | tr -d ' ')."
failed=0
while read -r name sha age; do
	prefix="ce-builds/${name}/${sha}/"
	tag="ce-${name}-${sha}"
	if [[ "${DRY_RUN:-}" == true ]]; then
		echo "[dry-run] Would delete ${prefix} and ${tag}[-dev] (last stored ${age}Z)."
		continue
	fi
	echo "Deleting ${prefix} and ${tag}[-dev] (last stored ${age}Z)."
	if ! aws s3 rm --quiet --recursive "s3://${BUCKET}/${prefix}"; then
		echo "::error::Could not delete s3://${BUCKET}/${prefix}."
		failed=$((failed + 1))
	fi
	if ! out="$(aws ecr batch-delete-image --region "${ECR_REGION}" --repository-name "${ECR_REPOSITORY}" \
		--image-ids "imageTag=${tag}" "imageTag=${tag}-dev" --output json)" ||
		jq -e '[.failures[]? | select(.failureCode != "ImageNotFound")] | length > 0' <<<"${out}" >/dev/null; then
		echo "::error::Could not delete the ECR images ${tag}[-dev]: ${out:-no response}"
		failed=$((failed + 1))
	fi
done <<<"${doomed}"

if ((failed > 0)); then
	echo "::error::${failed} deletion(s) failed; the sets they belong to are left, possibly in part."
	exit 1
fi
