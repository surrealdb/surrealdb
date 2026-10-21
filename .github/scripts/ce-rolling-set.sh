#!/usr/bin/env bash
# Names and inspects a community rolling set: one commit's build on one branch
# by ce-rolling-build.yml, stored as s3://<bucket>/ce-builds/<slug>/<sha>/ and
# the ECR tags ce-<slug>-<sha> and ce-<slug>-<sha>-dev. The rolling build, the
# release (ce-release.yml) and the cleanup (ce-prune.sh) all take the branch's
# slug and the set's contents from here, so they address the same objects.
#
#   slug <branch>  The branch's slug: every character outside [a-zA-Z0-9]
#                  becomes '-', and the rest is lowercased.
#   binaries       The binaries a set holds, as the suffixes of
#                  surreal-ce-<sha>., one per line: the rolling build's
#                  matrix plus the macOS universal binary.
#   missing        Prints what the set lacks, one "s3 <key>" or "image <tag>"
#                  line each, and exits 1 if it lacks anything: every binary,
#                  both images, and with NPM=true each npm bundle's ./dist/
#                  files that its package.json at SHA exports, and the Node
#                  bundle's native binary per napi target. Reads the manifests
#                  with `git show`, so it runs in a clone that has SHA.
#   digest         Prints a digest of what the set holds now: its S3 keys, and
#                  which of its images exist with their digests. A stored set
#                  only ever loses objects, so an unchanged digest proves
#                  nothing was deleted.
#
# missing and digest read BUCKET, PREFIX (ce-builds/<slug>/<sha>), TAG
# (ce-<slug>-<sha>), ECR_REGION, ECR_REPOSITORY, and missing also SHA and NPM,
# with ambient AWS credentials that can list the bucket and read the ECR
# repository. A failed read prints an error and exits 2.
set -euo pipefail

binaries() {
	printf '%s\n' \
		linux-amd64.tgz linux-amd64.txt linux-arm64.tgz linux-arm64.txt \
		darwin-amd64.tgz darwin-amd64.txt darwin-arm64.tgz darwin-arm64.txt \
		darwin-universal.tgz darwin-universal.txt \
		windows-amd64.exe windows-amd64.txt
}

# The keys under s3://BUCKET/$1, one per line. `aws s3 ls` exits 1 when
# nothing matches, which is an empty listing, and above 1 on a failure.
keys() {
	local out rc
	set +e
	out="$(aws s3 ls --recursive "s3://${BUCKET}/$1")"
	rc=$?
	set -e
	if ((rc > 1)); then
		echo "::error::Could not list s3://${BUCKET}/$1 (aws exited ${rc})." >&2
		exit 2
	fi
	awk 'NF == 4 { print $4 }' <<<"${out}"
}

# "<tag>\t<digest>" for each of TAG and TAG-dev that exists.
images() {
	if ! aws ecr batch-get-image --region "${ECR_REGION}" --repository-name "${ECR_REPOSITORY}" \
		--image-ids "imageTag=${TAG}" "imageTag=${TAG}-dev" \
		--accepted-media-types application/vnd.oci.image.index.v1+json \
		application/vnd.docker.distribution.manifest.list.v2+json \
		application/vnd.oci.image.manifest.v1+json \
		application/vnd.docker.distribution.manifest.v2+json \
		--query 'images[].[imageId.imageTag, imageId.imageDigest]' --output text; then
		echo "::error::Could not read the images ${TAG}[-dev] from ECR." >&2
		exit 2
	fi
}

case "${1:-}" in
slug)
	printf '%s\n' "${2:?usage: ce-rolling-set.sh slug <branch>}" | sed -e 's/[^a-zA-Z0-9]/-/g' | tr '[:upper:]' '[:lower:]'
	;;
binaries)
	binaries
	;;
missing)
	: "${BUCKET:?}" "${PREFIX:?}" "${TAG:?}" "${ECR_REGION:?}" "${ECR_REPOSITORY:?}" "${SHA:?}" "${NPM:?}"
	missing=()
	stored="$(keys "${PREFIX}/")"
	while IFS= read -r suffix; do
		key="${PREFIX}/surreal-ce-${SHA}.${suffix}"
		grep -qxF -- "${key}" <<<"${stored}" || missing+=("s3 ${key}")
	done <<<"$(binaries)"
	if [[ "${NPM}" == true ]]; then
		for slot in node wasm; do
			if ! manifest="$(git show "${SHA}:surrealdb/${slot}/package.json")"; then
				echo "::error::Could not read surrealdb/${slot}/package.json at ${SHA}." >&2
				exit 2
			fi
			# Every ./dist/ path anywhere in exports: a subpath's string, or a
			# (nested) condition's.
			if ! wanted="$(jq -r '[.exports | .. | strings | select(startswith("./dist/")) | sub("^\\./dist/"; "")] | unique | .[]' <<<"${manifest}")" || [[ -z "${wanted}" ]]; then
				echo "::error::surrealdb/${slot}/package.json at ${SHA} exports no ./dist/ file, so its bundle cannot be checked." >&2
				exit 2
			fi
			while IFS= read -r f; do
				key="${PREFIX}/npm/${slot}/${f}"
				grep -qxF -- "${key}" <<<"${stored}" || missing+=("s3 ${key}")
			done <<<"${wanted}"
			if [[ "${slot}" == node ]]; then
				want="$(jq '.napi.targets | length' <<<"${manifest}")"
				got="$(grep -c "^${PREFIX}/npm/node/.*\.node\$" <<<"${stored}" || true)"
				((got >= want)) || missing+=("s3 ${PREFIX}/npm/node/: ${got} of ${want} native binaries")
			fi
		done
	fi
	found="$(images)"
	for t in "${TAG}" "${TAG}-dev"; do
		awk -F'\t' -v t="${t}" '$1 == t { hit = 1 } END { exit !hit }' <<<"${found}" || missing+=("image ${t}")
	done
	if ((${#missing[@]} > 0)); then
		printf '%s\n' "${missing[@]}"
		exit 1
	fi
	;;
digest)
	: "${BUCKET:?}" "${PREFIX:?}" "${TAG:?}" "${ECR_REGION:?}" "${ECR_REPOSITORY:?}"
	stored="$(keys "${PREFIX}/")"
	found="$(images)"
	{
		awk 'NF { print "s3", $0 }' <<<"${stored}" | sort
		awk -F'\t' 'NF == 2 { print "image", $1, $2 }' <<<"${found}" | sort
	} | sha256sum | cut -d' ' -f1
	;;
*)
	echo "usage: ce-rolling-set.sh slug <branch> | binaries | missing | digest" >&2
	exit 2
	;;
esac
