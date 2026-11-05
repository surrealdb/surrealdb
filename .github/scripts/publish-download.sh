#!/usr/bin/env bash
# shellcheck disable=SC2153 # VERSION, NAME and RELEASE_NAME come from the caller
# Writes a release to the public download bucket, in the layout every download
# client reads, which is the database's own (see promote-binaries): the same
# pointers and file names, under a component's prefix. Used by
# publish-surreal-ext.yml; the database's release keeps its steps inline.
#
#   publish-download.sh latest           <prefix>latest.txt          -> v<version>
#   publish-download.sh major            <prefix>v<major>.txt        -> v<version>, only ever advancing
#   publish-download.sh channel          <prefix><release-name>.txt  -> v<version>
#   publish-download.sh binaries <dir>   every file in <dir>         -> <prefix><name>/
#
# Environment:
#   PREFIX        key prefix, empty or ending in `/` (the database's is empty)
#   VERSION       the version, without the `v` (e.g. 4.0.0 or 4.0.0-beta.1)
#   NAME          the release name the files live under (e.g. v4.0.0 or nightly)
#   RELEASE_NAME  the channel a pre-release pointer is named after (alpha, beta, nightly)
#   BUCKET        defaults to download.surrealdb.com
#   DRY_RUN       `true` prints each upload instead of making it, and reads no
#                 published pointer; it needs no AWS credentials
#
# Every object is written with `Cache-Control: no-store`. The pointer files are
# also written to the working directory under their own name.
set -euo pipefail

BUCKET="${BUCKET:-download.surrealdb.com}"
PREFIX="${PREFIX:-}"

if [[ -n "${PREFIX}" && "${PREFIX}" != */ ]]; then
	echo "::error::PREFIX must be empty or end in '/' (got '${PREFIX}')"
	exit 1
fi

# `aws s3 cp` with the cache policy every download object carries.
upload() {
	if [[ "${DRY_RUN:-false}" == "true" ]]; then
		echo "would upload $1 -> $2"
	else
		aws s3 cp --cache-control 'no-store' "$1" "$2"
	fi
}

require() {
	local var
	for var in "$@"; do
		if [[ -z "${!var:-}" ]]; then
			echo "::error::${var} must be set"
			exit 1
		fi
	done
}

case "${1:-}" in
	latest)
		require VERSION
		echo "v${VERSION}" > latest.txt
		upload latest.txt "s3://${BUCKET}/${PREFIX}latest.txt"
		;;
	major)
		require VERSION
		version="${VERSION}"
		major="${version%%.*}"
		# The pointer names the newest stable on this major line for download
		# clients, so it must only ever advance: a patch release on an OLDER
		# minor (e.g. v3.1.x shipped after v3.2.x) keeps the published pointer
		# where it is. Only stable X.Y.Z triples reach this step, so a plain
		# version sort decides; an equal version rewrites (a re-release stays
		# idempotent) and a missing pointer is created.
		current=""
		if [[ "${DRY_RUN:-false}" != "true" ]]; then
			current="$(aws s3 cp "s3://${BUCKET}/${PREFIX}v${major}.txt" - 2>/dev/null | tr -d 'v[:space:]' || true)"
		fi
		if [ -n "${current}" ] \
			&& [ "$(printf '%s\n' "${current}" "${version}" | sort -V | tail -n1)" != "${version}" ]; then
			echo "Keeping ${PREFIX}v${major}.txt at v${current}: released v${version} is older."
			exit 0
		fi
		echo "v${version}" > "v${major}.txt"
		upload "v${major}.txt" "s3://${BUCKET}/${PREFIX}v${major}.txt"
		;;
	channel)
		require VERSION RELEASE_NAME
		echo "v${VERSION}" > "${RELEASE_NAME}.txt"
		upload "${RELEASE_NAME}.txt" "s3://${BUCKET}/${PREFIX}${RELEASE_NAME}.txt"
		;;
	binaries)
		require NAME
		dir="${2:?usage: publish-download.sh binaries <dir>}"
		# An empty directory is a broken build, not a release with nothing in
		# it: stop with a clear error rather than upload a literal `*`, or
		# nothing at all.
		shopt -s nullglob
		files=("${dir}"/*)
		shopt -u nullglob
		if [ "${#files[@]}" -eq 0 ]; then
			echo "error: nothing to publish in ${dir}" >&2
			exit 1
		fi
		for file in "${files[@]}"; do
			upload "$file" "s3://${BUCKET}/${PREFIX}${NAME}/"
		done
		;;
	*)
		echo "usage: publish-download.sh latest|major|channel|binaries <dir>" >&2
		exit 2
		;;
esac
