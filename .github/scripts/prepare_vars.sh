#!/bin/bash

set -ux

RELEASE_TYPE="${RELEASE_TYPE}"
GITHUB_REF="${GITHUB_REF}"
GITHUB_OUTPUT="${GITHUB_OUTPUT}"

suffix=$(echo ${RELEASE_TYPE} | tr "." "\n" | sed -n 1p)
patch=$(echo ${RELEASE_TYPE} | tr "." "\n" | sed -n 2p)
current_version=$(cargo metadata --format-version 1 --no-deps | jq -r '.packages | map(select(.name == "surrealdb"))[0].version')

if [[ $current_version == *"-"* ]]; then
    echo "Found an unexpected pre-release version, '${current_version}', in ${GITHUB_REF}"
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
        patch=$(echo ${current_version} | tr "." "\n" | sed -n 3p)
        environment=stable
        ;;
    "patch")
        major=$(echo ${current_version} | tr "." "\n" | sed -n 1p)
        minor=$(echo ${current_version} | tr "." "\n" | sed -n 2p)
        currentPatch=$(echo ${current_version} | tr "." "\n" | sed -n 3p)
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
        version=${current_version}-${RELEASE_TYPE}
        environment=${RELEASE_TYPE}
        ;;
    "rc")
        version=${current_version}-${RELEASE_TYPE}
        patch=$(($patch - 1))
        environment=rc
        ;;
    *)
        version=${current_version}-${RELEASE_TYPE}
        patch=$(($patch - 1))
        environment=${suffix}
        ;;
esac

# Output the variables to the GitHub Actions environment.

echo "current-version=${current_version}" >> $GITHUB_OUTPUT
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
