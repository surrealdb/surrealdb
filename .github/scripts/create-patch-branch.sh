#!/usr/bin/env bash
set -e

VERSION="$1"
PUBLISH="${2:-false}"

if [[ -z "$VERSION" ]]; then
	echo "Error: Version argument required"
	echo "Usage: $0 <version> [publish]"
	exit 1
fi

# Check if this is a stable x.y.0 release (no pre-release, patch = 0)
major=$(echo $VERSION | tr "." "\n" | sed -n 1p)
minor=$(echo $VERSION | tr "." "\n" | sed -n 2p)
patch=$(echo $VERSION | tr "." "\n" | sed -n 3p)

# Check if patch part is just "0" (no pre-release suffix like "0-beta")
if [[ "$patch" == "0" ]] && [[ ! "$VERSION" =~ - ]]; then
	PATCH_BRANCH="release/${major}.${minor}"
	echo "Stable x.y.0 release detected: creating long-lived patch branch ${PATCH_BRANCH}"

	# Delete patch branch if it exists (idempotency)
	if git ls-remote --exit-code --heads origin "${PATCH_BRANCH}" >/dev/null 2>&1; then
		echo "Patch branch ${PATCH_BRANCH} already exists, deleting it"
		git push origin --delete "${PATCH_BRANCH}" || true
	fi
	if git show-ref --verify --quiet "refs/heads/${PATCH_BRANCH}"; then
		git branch -D "${PATCH_BRANCH}"
	fi

	# Create the long-lived branch for future patches
	git checkout -b "${PATCH_BRANCH}"

	# Only push if publishing
	if [[ "$PUBLISH" == "true" ]]; then
		git push origin "${PATCH_BRANCH}"
		echo "Pushed ${PATCH_BRANCH} for future patch releases (${major}.${minor}.1, ${major}.${minor}.2, etc.)"
	else
		echo "[Dry-run] Would push ${PATCH_BRANCH} for future patch releases"
	fi
else
	echo "Not a stable x.y.0 release, skipping patch branch creation"
fi

