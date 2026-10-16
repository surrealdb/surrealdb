#!/usr/bin/env bash

# Checks that a promotion publishes the release layout the `surreal` launcher
# (surrealdb/surrealdb-cli) reads, before anything is uploaded.
#
#   check-release-layout.sh <publish dir> <pointers dir> <name> <version> <pre-release> <release-name>
#
# The launcher installs the database from download.surrealdb.com and shares no
# code with this repository: this layout is the whole interface, and every
# installed launcher depends on it, so it can only grow. The contract:
#
#   * Pointers at the bucket root (latest, v1, v2, v3, ..., and one per
#     prerelease channel: alpha, beta, nightly): `<pointer>.txt`, holding
#     `v<version>` and nothing else. A stable release sets `v<major>`, and
#     `latest` when it is the latest; a prerelease sets its channel's pointer.
#   * `<name>/surreal-<name>.<platform>.tgz` holding exactly one executable
#     member named `surreal`, with `surreal-<name>.<platform>.txt` beside it
#     holding the archive's SHA-256 in hex. Windows publishes the `.exe` itself.
#   * `surreal --version` with SURREAL_ONLINE_VERSION_CHECK=false exits 0 and
#     prints the version, touching no network and no data.
#
# <publish dir> is what the promotion is about to upload to <name>/ (after the
# rolling artifacts are renamed to the release name). <pointers dir> holds one
# file per pointer the promotion is about to write. Both are checked as staged,
# so this runs on a dry-run too. The launcher repository's
# `scripts/check-db-releases.sh` checks the same layout from the other side,
# against what is published.

set -euo pipefail

[ $# -eq 6 ] || {
    echo "usage: $0 <publish dir> <pointers dir> <name> <version> <pre-release> <release-name>" >&2
    exit 2
}
publish=$1
pointers=$2
name=$3
version=$4
pre_release=$5
release_name=$6

failures=0
fail() {
    echo "::error title=Release layout::$1"
    failures=$((failures + 1))
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -f1 -d' '
    else
        shasum -a 256 "$1" | cut -f1 -d' '
    fi
}

# What the launcher can install from. A platform missing here is a platform
# the launcher cannot serve, so a promotion without one is refused.
tgz_platforms="linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 darwin-universal"
exe_platforms="windows-amd64"

# --- Pointers ----------------------------------------------------------------

pointer_re='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'

# The pointer a promotion exists to move, and the only other one it may move.
if [ "$pre_release" = true ]; then
    primary="${release_name}.txt"
else
    primary="v${version%%.*}.txt"
fi

shopt -s nullglob
found=("${pointers}"/*)
[ -f "${pointers}/${primary}" ] || fail "the promotion does not write ${primary}"
for file in "${found[@]}"; do
    base=$(basename "$file")
    if [ "$base" != "$primary" ] && [ "$base" != latest.txt ]; then
        fail "pointer ${base} is not one the promotion should write (${primary}, or latest.txt)"
    fi
    content=$(cat "$file")
    if ! [[ "$content" =~ $pointer_re ]]; then
        fail "pointer ${base} holds \`${content}\`, not v<major>.<minor>.<patch>[-<prerelease>]"
    elif [ "$content" != "v${version}" ]; then
        fail "pointer ${base} holds \`${content}\`, not v${version}"
    else
        echo "ok  ${base} -> ${content}"
    fi
done

# --- Archives ----------------------------------------------------------------

# The directory a release lives under is its name: `v<version>` (plus build
# metadata on a test release), or the one mutable `nightly`.
if [ "$name" != nightly ] && [[ "$name" != "v${version}" && "$name" != "v${version}+"* ]]; then
    fail "release name ${name} is neither nightly nor v${version}"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

check_sidecar() {
    local artifact=$1 sidecar=$2
    if [ ! -f "$sidecar" ]; then
        fail "$(basename "$artifact") has no SHA-256 beside it (expected $(basename "$sidecar"))"
        return 1
    fi
    # Hex and a line ending. CRLF is tolerated because the Windows build writes
    # certutil's output; the launcher trims it too.
    local digest
    digest=$(tr -d '\r\n' <"$sidecar")
    if ! [[ "$digest" =~ ^[0-9a-fA-F]{64}$ ]]; then
        fail "$(basename "$sidecar") holds \`$(head -c 100 "$sidecar")\`, not a hex SHA-256"
        return 1
    fi
    if [ "$(echo "$digest" | tr 'A-F' 'a-f')" != "$(sha256 "$artifact")" ]; then
        fail "$(basename "$artifact") does not match $(basename "$sidecar")"
        return 1
    fi
    echo "ok  $(basename "$artifact") matches $(basename "$sidecar")"
}

for platform in $tgz_platforms; do
    archive="${publish}/surreal-${name}.${platform}.tgz"
    if [ ! -f "$archive" ]; then
        fail "$(basename "$archive") is missing: the archive name or platform has changed"
        continue
    fi
    check_sidecar "$archive" "${archive%.tgz}.txt" || true
    members=$(tar -tzf "$archive")
    if [ "$members" != "surreal" ]; then
        fail "$(basename "$archive") must hold exactly one member named surreal, holds: $(echo "$members" | tr '\n' ' ')"
        continue
    fi
    mode=$(tar -tvzf "$archive" | cut -c1-10)
    case "$mode" in
        -rwx*) echo "ok  $(basename "$archive") holds one executable, surreal" ;;
        *) fail "$(basename "$archive"): surreal is not executable (${mode})" ;;
    esac
done

for platform in $exe_platforms; do
    exe="${publish}/surreal-${name}.${platform}.exe"
    if [ ! -f "$exe" ]; then
        fail "$(basename "$exe") is missing: the Windows executable name has changed"
        continue
    fi
    check_sidecar "$exe" "${exe%.exe}.txt" || true
done

# Anything else staged is published but not read by the launcher; say so, so a
# stray rename does not hide behind the files that are fine.
for file in "${publish}"/*; do
    base=$(basename "$file")
    known=
    for platform in $tgz_platforms; do
        case "$base" in "surreal-${name}.${platform}.tgz" | "surreal-${name}.${platform}.txt") known=1 ;; esac
    done
    for platform in $exe_platforms; do
        case "$base" in "surreal-${name}.${platform}.exe" | "surreal-${name}.${platform}.txt") known=1 ;; esac
    done
    [ -n "$known" ] || echo "::notice title=Release layout::${base} is published but not read by the launcher"
done

# --- The binary ---------------------------------------------------------------

# Run the archive that matches this runner. Online version check off, an empty
# home and working directory, and no network where the runner lets us drop it:
# the launcher runs `surreal --version` to identify an install, so it must not
# fetch or write anything.
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) host=linux-amd64 ;;
    Linux-aarch64) host=linux-arm64 ;;
    Darwin-arm64) host=darwin-arm64 ;;
    Darwin-x86_64) host=darwin-amd64 ;;
    *) host= ;;
esac
if [ -z "$host" ]; then
    echo "::notice title=Release layout::no archive runs on this runner; skipping the --version check"
elif [ -f "${publish}/surreal-${name}.${host}.tgz" ] && [ "$failures" -eq 0 ]; then
    mkdir -p "${work}/bin" "${work}/home" "${work}/cwd"
    tar -xzf "${publish}/surreal-${name}.${host}.tgz" -C "${work}/bin"
    isolate=()
    if command -v unshare >/dev/null 2>&1 && unshare -rn true 2>/dev/null; then
        isolate=(unshare -rn)
    else
        echo "::notice title=Release layout::cannot drop the network on this runner; --version runs with it"
    fi
    status=0
    output=$(cd "${work}/cwd" && env -i PATH=/usr/bin:/bin HOME="${work}/home" \
        SURREAL_ONLINE_VERSION_CHECK=false \
        ${isolate[@]+"${isolate[@]}"} "${work}/bin/surreal" --version 2>&1) || status=$?
    core=${version%%-*}
    if [ "$status" -ne 0 ]; then
        fail "surreal --version exited ${status}: ${output}"
    elif [[ "$output" != *"$core"* ]]; then
        fail "surreal --version prints \`${output}\`, which does not mention ${core}"
    elif [ -n "$(find "${work}/cwd" "${work}/home" -mindepth 1)" ]; then
        fail "surreal --version wrote files: $(find "${work}/cwd" "${work}/home" -mindepth 1 | tr '\n' ' ')"
    else
        echo "ok  surreal --version exits 0 and prints \`${output}\`"
    fi
fi

if [ "$failures" -gt 0 ]; then
    echo "::error title=Release layout::the promotion would break the layout the surreal launcher reads (${failures} problems); see .github/actions/promote-binaries"
    exit 1
fi
echo "The release layout the surreal launcher reads is intact."
