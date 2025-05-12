#!/bin/bash

set -euo pipefail

ENVIRONMENT="${ENVIRONMENT}"
CURRENT_VERSION="${CURRENT_VERSION}"
VERSION="${VERSION}"
PATCH="${PATCH}"

# Note: Keep these in sync with the `members` array in the `Cargo.toml` file.
members=("surrealdb-common" "surrealdb-expr" "surrealdb-core" "surrealdb-sql" "surrealdb")
member_paths=("crates/common" "crates/expr" "crates/core" "crates/sql" "crates/sdk")

# Get the path of a crate based on its name
# Usage: get_member_path <crate_name>
# Example: get_member_path "surrealdb-core"
# This function will search for the crate name in the members array and return its corresponding path
# If the crate name is not found, it will print an error message and exit with a non-zero status
function get_member_path() {
    local crate_name=$1
    set +u
    local crate_path
    local index=0

    for member_name in "${members[@]}"; do
        if [[ $member_name == $crate_name ]]; then
            crate_path="${member_paths[$index]}"
            break
        fi
        index=$((index + 1))
    done

    if [[ -z $crate_path ]]; then
        echo "Error: No path found for crate name '$crate_name'"
        exit 1
    fi

    echo $crate_path
    set -u
}

# This function updates the version in the Cargo.toml file and pins the version in the member crates.
function patch_version() {
    major=$(echo $CURRENT_VERSION | tr "." "\n" | sed -n 1p)
    minor=$(echo $CURRENT_VERSION | tr "." "\n" | sed -n 2p)
    new_version=${major}.${minor}.${PATCH}

    sed -i "s#^version = \".*\"#version = \"${new_version}\"#" ./Cargo.toml

    for crate_name in "${members[@]}"; do
        echo "${crate_name}"
        crate_path="$(get_member_path $crate_name)"
        sed -i "s#^${crate_name} = { version = \"=${VERSION}\"#${crate_name} = { version = \"=${new_version}\"#" Cargo.toml
    done
}

# This function updates the name of the crate in the Cargo.toml file to include the environment.
function patch_name() {
    for crate_name in "${members[@]}"; do
        crate_path="$(get_member_path $crate_name)"
        echo "${crate_name} -> ${crate_path}"
        set -x
        sed -i "0,/name = \"${crate_name}\"/s//name = \"${crate_name}-${ENVIRONMENT}\"/" ${crate_path}/Cargo.toml
        set +x
    done
}

# This function updates the description in the Cargo.toml file to include the environment.
function patch_description() {
    if [[ $ENVIRONMENT == 'alpha' ]]; then
        start="An"
    else
        start="A"
    fi
    sed -i "s#^description = \".*\"#description = \"${start} ${ENVIRONMENT} release of the surrealdb crate\"#" Cargo.toml
}

case "${ENVIRONMENT}" in
    "stable")
        echo "Stable release, no patching required"
        ;;
    *)
        patch_version
        patch_name
        patch_description
        ;;
esac
