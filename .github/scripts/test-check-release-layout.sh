#!/usr/bin/env bash

# Exercises check-release-layout.sh against a staged release it should accept,
# then against the same release with one thing broken at a time, each of which
# it must refuse. Run by CI; needs nothing but bash and tar.

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check="${here}/check-release-layout.sh"
version=3.1.2
name=v${version}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -f1 -d' '
    else
        shasum -a 256 "$1" | cut -f1 -d' '
    fi
}

# A release as the promotion stages it: every archive holds one executable
# `surreal`, every artifact has its SHA-256 beside it, and the pointers are
# `latest` and `v3`, as for a stable release that is the latest.
stage() {
    local dir=$1 member=${2:-surreal}
    mkdir -p "${dir}/publish" "${dir}/pointers" "${dir}/member"
    printf '#!/bin/sh\necho "surreal %s for test"\n' "$version" >"${dir}/member/${member}"
    chmod 0755 "${dir}/member/${member}"
    local platform
    for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 darwin-universal; do
        local archive="${dir}/publish/surreal-${name}.${platform}.tgz"
        tar -czf "$archive" -C "${dir}/member" "$member"
        sha256 "$archive" >"${archive%.tgz}.txt"
    done
    local exe="${dir}/publish/surreal-${name}.windows-amd64.exe"
    echo MZ >"$exe"
    sha256 "$exe" >"${exe%.exe}.txt"
    echo "v${version}" >"${dir}/pointers/latest.txt"
    echo "v${version}" >"${dir}/pointers/v3.txt"
}

run() {
    "$check" "${1}/publish" "${1}/pointers" "$name" "$version" false stable >"${1}/out" 2>&1
}

failed=0
expect_ok() {
    stage "${root}/$1"
    if ! run "${root}/$1"; then
        echo "FAIL: a correct release was refused"
        cat "${root}/$1/out"
        failed=1
    fi
}
# expect_refused <case> <what is broken> <command run in the staged dir>
expect_refused() {
    local dir="${root}/$1"
    stage "$dir"
    (cd "$dir" && eval "$3")
    if run "$dir"; then
        echo "FAIL: ${2} was not refused"
        failed=1
    else
        echo "ok   refused: ${2}"
    fi
}

expect_ok good
echo "ok   accepted: a correct release"

# The member is renamed inside the archives, so stage with another name.
rm -rf "${root}/member"
stage "${root}/member" surreal-db
if run "${root}/member"; then
    echo "FAIL: the archive member renamed was not refused"
    failed=1
else
    echo "ok   refused: the archive member renamed to surreal-db"
fi

expect_refused pointer_name "a pointer renamed" \
    'mv pointers/latest.txt pointers/newest.txt'
expect_refused pointer_missing "the major pointer not written" \
    'rm pointers/v3.txt'
expect_refused pointer_content "a pointer holding the wrong version" \
    'echo v9.9.9 >pointers/v3.txt'
expect_refused pointer_format "a pointer without its v prefix" \
    "echo ${version} >pointers/latest.txt"
expect_refused archive_name "an archive renamed" \
    "mv publish/surreal-${name}.linux-amd64.tgz publish/surrealdb-${name}.linux-amd64.tgz"
expect_refused sidecar_missing "a SHA-256 missing" \
    "rm publish/surreal-${name}.linux-arm64.txt"
expect_refused sidecar_wrong "a SHA-256 that does not match" \
    "printf '%064d\n' 0 >publish/surreal-${name}.darwin-arm64.txt"
expect_refused sidecar_format "a SHA-256 in another format" \
    "echo 'SHA256=x' >publish/surreal-${name}.linux-amd64.txt"
expect_refused windows_exe "the Windows executable renamed" \
    "mv publish/surreal-${name}.windows-amd64.exe publish/surreal-${name}.windows-x64.exe"

# A prerelease moves its channel's pointer, not the stable ones.
pre="${root}/prerelease"
stage "$pre"
rm "${pre}/pointers/v3.txt" "${pre}/pointers/latest.txt"
echo "v${version}" >"${pre}/pointers/beta.txt"
if "$check" "${pre}/publish" "${pre}/pointers" "$name" "$version" true beta >"${pre}/out" 2>&1; then
    echo "ok   accepted: a prerelease moving its own pointer"
else
    echo "FAIL: a correct prerelease was refused"
    cat "${pre}/out"
    failed=1
fi
if "$check" "${pre}/publish" "${pre}/pointers" "$name" "$version" true alpha >"${pre}/out" 2>&1; then
    echo "FAIL: a prerelease pointer renamed was not refused"
    failed=1
else
    echo "ok   refused: a prerelease pointer renamed"
fi

# A binary whose --version fails must be refused too.
bad="${root}/version"
stage "$bad"
printf '#!/bin/sh\necho "no"; exit 3\n' >"${bad}/member/surreal"
for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 darwin-universal; do
    archive="${bad}/publish/surreal-${name}.${platform}.tgz"
    tar -czf "$archive" -C "${bad}/member" surreal
    sha256 "$archive" >"${archive%.tgz}.txt"
done
if run "$bad"; then
    echo "FAIL: a binary whose --version fails was not refused"
    failed=1
else
    echo "ok   refused: a binary whose --version fails"
fi

exit "$failed"
