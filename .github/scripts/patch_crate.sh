#!/bin/bash

set -x

ENVIRONMENT="${ENVIRONMENT}"
CURRENT_VERSION="${CURRENT_VERSION}"
VERSION="${VERSION}"
PATCH="${PATCH}"

function patch_version() {
    major=$(echo $CURRENT_VERSION | tr "." "\n" | sed -n 1p)
    minor=$(echo $CURRENT_VERSION | tr "." "\n" | sed -n 2p)
    new_version=${major}.${minor}.${PATCH}

    sed -i "s#^version = \".*\"#version = \"${new_version}\"#" Cargo.toml
    sed -i "s#surrealdb = { version = \"=${VERSION}\"#surrealdb = { version = \"=${new_version}\"#" Cargo.toml
    sed -i "s#surrealdb-core = { version = \"=${VERSION}\"#surrealdb-core = { version = \"=${new_version}\"#" Cargo.toml
}

function patch_name() {
    sed -i "0,/surrealdb/s//surrealdb-${ENVIRONMENT}/" crates/sdk/Cargo.toml
    sed -i "0,/surrealdb-core/s//surrealdb-core-${ENVIRONMENT}/" crates/core/Cargo.toml
}

function patch_description() {
    if [[ $ENVIRONMENT == 'alpha' ]]; then
        start="An"
    else
        start="A"
    fi
    sed -i "s#^description = \".*\"#description = \"${start} ${ENVIRONMENT} release of the surrealdb crate\"#" Cargo.toml
}

case $ENVIRONMENT in
    "stable")
        echo "Stable release, no patching required"
        ;;
    *)
        patch_version()
        patch_name()
        patch_description()
        ;;
esac
