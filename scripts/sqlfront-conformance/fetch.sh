#!/bin/bash
# Fetches the conformance data pinned by <lockfile> into
# ${CARGO_TARGET_DIR:-<repo>/target}/sql-conformance/<commit>/ and verifies every
# fetched file against the pinned MANIFEST.sha256. A tree that already verifies
# is only re-hashed. Never edits the lock; exits non-zero on any mismatch.
set -euo pipefail
lock=${1:?usage: fetch.sh <lockfile>}
val() { sed -n "s/^$1 = \"\(.*\)\"\$/\1/p" "$lock" | head -n1; }
repo=$(val repo); commit=$(val commit); manifest_sha=$(val manifest_sha256)
[[ $commit =~ ^[0-9a-f]{40}$ && $manifest_sha =~ ^[0-9a-f]{64}$ ]] || { echo "malformed lock: $lock" >&2; exit 2; }
paths=(); while IFS= read -r p; do paths+=("$p"); done < <(sed -n 's/^path = "\(.*\)"$/\1/p' "$lock")
((${#paths[@]} > 0)) || { echo "lock lists no datasets: $lock" >&2; exit 2; }
root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
dest="${CARGO_TARGET_DIR:-$root/target}/sql-conformance/$commit"
sha() { if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
verify() (
	cd "$1"
	[[ $(sha MANIFEST.sha256 | cut -c1-64) == "$manifest_sha" ]] || exit 1
	for p in "${paths[@]}"; do grep -F "  $p/" MANIFEST.sha256 || exit 1; done | sha -c --quiet --strict -
)
if [[ -f $dest/.verified ]] && verify "$dest"; then exit 0; fi
mkdir -p "$(dirname "$dest")"
tmp=$(mktemp -d "$dest.partial.XXXXXX"); trap 'rm -rf "$tmp"' EXIT
git -C "$tmp" init -q
git -C "$tmp" remote add origin "$repo"
git -C "$tmp" sparse-checkout set --no-cone /MANIFEST.sha256 "${paths[@]/#//}"
git -C "$tmp" -c protocol.version=2 fetch -q --depth 1 --filter=blob:none origin "$commit"
git -C "$tmp" checkout -q --detach FETCH_HEAD
[[ $(git -C "$tmp" rev-parse HEAD) == "$commit" ]]
verify "$tmp" || { echo "conformance data failed verification against $lock" >&2; exit 1; }
rm -rf "$tmp/.git"; echo "$manifest_sha" > "$tmp/.verified"; rm -rf "$dest"; mv "$tmp" "$dest"; trap - EXIT
