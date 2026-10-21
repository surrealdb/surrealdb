#!/usr/bin/env bash
# Claims a versioned engine release for the attempt that is about to publish
# it: refuses a completed release unless it is being overwritten, and binds the
# version to the commit it publishes, refusing any other commit once bound.
#
# A release that completed is marked by its manifest,
# s3://<bucket>/releases/<name>/engine.json. The overwrite guard in each
# workflow's prepare job checks it before approval; this checks it again after,
# because another release of the version (from either workflow) can complete
# while this one waits at the gate.
#
# Crates and npm packages are write-once, so a release that stopped part-way
# keeps what it published, and a retry from another commit carrying the same
# version would finish it with that commit's binaries and images. The binding
# is releases/<name>/engine.sha, written by every approved attempt from either
# release workflow (release.yml or ce-release.yml). A completion manifest's
# git-sha binds the version the same way. Once either names a commit, every
# later attempt, overwrite included, must release that commit.
#
# Because every attempt rewrites engine.sha and a completed one then rewrites
# engine.json, a binding newer than the manifest marks an attempt, overwrite
# included, that has not completed. The weekly cleanup of rolling sets
# (ce-prune.sh) keeps that commit's set so the attempt can be re-run.
#
# Each workflow calls this from its approval job, after approval and before
# anything publishes, once release-publishing.sh has found no other approved
# run of either workflow that may be publishing this version. That is what
# keeps two attempts from both finding the version unclaimed.
#
# Reads NAME (the release name, e.g. v3.3.1), GIT_SHA, OVERWRITE ('true'
# republishes a completed release), PUBLISH ('true' writes the binding;
# anything else only checks), BUCKET, RUNNER_TEMP and ambient AWS credentials
# that can read and write the bucket. Only a missing object counts as absent;
# any other S3 error fails, so an unreadable manifest or binding is never
# taken for none.
set -euo pipefail

: "${NAME:?}" "${GIT_SHA:?}" "${BUCKET:?}" "${RUNNER_TEMP:?}"

# Copies key $1 to file $2; false when the key does not exist.
fetch() {
	local err
	if err=$(aws s3api get-object --bucket "${BUCKET}" --key "$1" "$2" 2>&1 >/dev/null); then
		return 0
	fi
	if grep -q 'NoSuchKey' <<<"${err}"; then
		return 1
	fi
	echo "::error::Could not read s3://${BUCKET}/$1 (${err})."
	exit 1
}

manifest_sha=""
if fetch "releases/${NAME}/engine.json" "${RUNNER_TEMP}/manifest"; then
	if [[ "${OVERWRITE:-}" != true ]]; then
		echo "::error title=${NAME} is already released::${NAME} completed while this release waited, or before it (manifest at s3://${BUCKET}/releases/${NAME}/engine.json). Bump the version, or re-run with overwrite to republish it."
		exit 1
	fi
	manifest_sha=$(jq -r '."git-sha" // ""' "${RUNNER_TEMP}/manifest")
fi

bound="${manifest_sha}"
if fetch "releases/${NAME}/engine.sha" "${RUNNER_TEMP}/bound"; then
	bound=$(tr -d '[:space:]' <"${RUNNER_TEMP}/bound")
fi

if [[ -n "${bound}" && "${bound}" != "${GIT_SHA}" ]]; then
	echo "::error title=${NAME} is bound to another commit::${NAME} was first released from ${bound}, and what that attempt published to crates.io and npm cannot be replaced. Finish it by releasing ${bound}, or bump the version to release ${GIT_SHA}."
	exit 1
fi
if [[ "${PUBLISH:-}" != true ]]; then
	if [[ -n "${bound}" ]]; then
		echo "${NAME} is bound to ${GIT_SHA}."
	else
		echo "[dry-run] Would bind ${NAME} to ${GIT_SHA}."
	fi
	exit 0
fi
printf '%s\n' "${GIT_SHA}" | aws s3 cp - "s3://${BUCKET}/releases/${NAME}/engine.sha"
if [[ -n "${bound}" ]]; then
	echo "${NAME} is bound to ${GIT_SHA}; recorded this attempt."
else
	echo "Bound ${NAME} to ${GIT_SHA}."
fi
