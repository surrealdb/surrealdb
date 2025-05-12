#!/bin/bash

set -ux

RELEASE_TYPE=$1
GITHUB_REF=$2
GITHUB_OUTPUT="${GITHUB_OUTPUT}"

suffix=$(echo ${RELEASE_TYPE} | tr "." "\n" | sed -n 1p)
patch=$(echo ${RELEASE_TYPE} | tr "." "\n" | sed -n 2p)
version=$(cargo metadata --format-version 1 --no-deps | jq -r '.packages | map(select(.name == "surrealdb"))[0].version')
echo "current-version=${version}" >> $GITHUB_OUTPUT

if [[ $version == *"-"* ]]; then
echo "Found an unexpected pre-release version, '${version}', in ${GITHUB_REF}"
exit 400
fi

if [[ "${RELEASE_TYPE}" != "release" && "${RELEASE_TYPE}" != "patch" && "${RELEASE_TYPE}" != "nightly" && $suffix != "alpha" && $suffix != "beta" && $suffix != "rc" ]]; then
echo "'${RELEASE_TYPE}' is not a supported release type"
exit 400
fi

if [[ $suffix == "alpha" || $suffix == "beta" || $suffix == "rc" ]]; then
if [[ -z $patch ]]; then
    echo "Pre-releases require a patch number, e.g. beta.3"
    exit 400
elif ! [[ $patch =~ ^[0-9]+$ ]]; then
    echo "The patch number should be an integer, found ${patch}"
    exit 400
fi
fi


case $suffix in
    "release")
        patch=$(echo ${version} | tr "." "\n" | sed -n 3p)
        environment=stable
        ;;
    "patch")
        major=$(echo ${version} | tr "." "\n" | sed -n 1p)
        minor=$(echo ${version} | tr "." "\n" | sed -n 2p)
        currentPatch=$(echo ${version} | tr "." "\n" | sed -n 3p)
        patch=$(($currentPatch + 1))
        version=${major}.${minor}.${patch}
        environment=stable
        ;;
    "nightly")
        date=$(git show --no-patch --format=%ad --date=format:%Y%m%d)
        # This sets the nightly version to something like `1.3.20250224221932`
        patch=$(git show --no-patch --format=%ad --date=format:%Y%m%d%H%M%S)
        rev=$(git rev-parse --short HEAD)
        buildMetadata=${date}.${rev}
        version=${version}-${RELEASE_TYPE}
        environment=${RELEASE_TYPE}
        ;;
    "rc")
        version=${version}-${RELEASE_TYPE}
        patch=$(($patch - 1))
        environment=release-candidate
        ;;
    *)
        version=${version}-${RELEASE_TYPE}
        patch=$(($patch - 1))
        environment=${suffix}
        ;;
esac

echo "version=${version}" >> $GITHUB_OUTPUT
echo "patch=${patch}" >> $GITHUB_OUTPUT
echo "environment=${environment}" >> $GITHUB_OUTPUT
echo "build-metadata=${buildMetadata}" >> $GITHUB_OUTPUT
echo "release-branch=releases/${version}" >> $GITHUB_OUTPUT

if [[ "${environment}" == "nightly" ]]; then
echo "name=${environment}" >> $GITHUB_OUTPUT
else
echo "name=v${version}" >> $GITHUB_OUTPUT
fi