#!/usr/bin/env bash
set -euo pipefail

# Cut the downstream counterpart of a new engine release line.
#
# A new engine releases/X.Y line needs a same-named branch on the downstream
# repo whose engine git dependencies track releases/X.Y instead of main: the
# rolling downstream build resolves the MATCHING release-line branch (falling
# back to main with a warning), and the release-time reproducibility tag
# (tag-downstream-release.sh) pins on top of that branch's manifests.
#
# Run from a checkout of the DOWNSTREAM repo's main branch. cargo must be able
# to fetch the engine repo (the call site rewrites the manifests' ssh:// engine
# URL to an authenticated https one and sets CARGO_NET_GIT_FETCH_WITH_CLI), and
# on publish the engine release branch must already exist - the engine side of
# the cut pushes first.
#
# Usage: cut-downstream-release.sh <releases/X.Y> [publish]
#   publish=false rewrites the manifests and reports, but skips the re-lock
#   (the engine branch does not exist on a dry-run) and pushes nothing.
#
# Env: ENGINE_REPO = the engine `owner/repo`, used to address engine deps.

RELEASE_BRANCH="${1:-}"
PUBLISH="${2:-false}"

if [[ ! "$RELEASE_BRANCH" =~ ^releases/[0-9]+\.[0-9]+$ ]]; then
	echo "Error: expected a releases/X.Y branch name, got '${RELEASE_BRANCH}'"
	exit 1
fi
if [[ -z "${ENGINE_REPO:-}" ]]; then
	echo "Error: ENGINE_REPO must be set (engine owner/repo)"
	exit 1
fi
ENGINE_MATCH="${ENGINE_REPO}.git"

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

BASE_SHA=$(git rev-parse HEAD)

# Idempotency: an existing branch is accepted ONLY when its manifests already
# track the release branch (a prior run of this cut, or a manual cut); anything
# else is a real pre-existing branch that must never be clobbered. The probe
# PARSES each manifest in the branch's tree and accepts only when an ENGINE
# dependency's own table carries `branch = <release branch>`: a textual match
# cannot pair the two keys reliably (the rewrite paths this validates can place
# engine deps in member manifests, split tables across lines, or vary spacing,
# and a same-named release branch of another repo in the same manifest must
# not count). A manifest that fails to parse counts as not-tracking, which
# lands on the refuse path - never on reuse.
tracks_engine_branch() {
	local f
	while IFS= read -r f; do
		[[ -z "$f" ]] && continue
		if git show "FETCH_HEAD:${f}" 2>/dev/null | ENGINE_MATCH="$ENGINE_MATCH" RELEASE_BRANCH="$RELEASE_BRANCH" python3 -c '
import os, sys, tomllib

def tables(node):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from tables(v)
    elif isinstance(node, list):
        for v in node:
            yield from tables(v)

try:
    data = tomllib.load(sys.stdin.buffer)
except Exception:
    sys.exit(1)
engine = os.environ["ENGINE_MATCH"]
branch = os.environ["RELEASE_BRANCH"]
for t in tables(data):
    if engine in str(t.get("git", "")) and t.get("branch") == branch:
        sys.exit(0)
sys.exit(1)
'; then
			return 0
		fi
	done < <(git grep -l -e "${ENGINE_MATCH}" FETCH_HEAD -- '*Cargo.toml' 2>/dev/null | sed 's/^FETCH_HEAD://')
	return 1
}
if git ls-remote --exit-code --heads origin "${RELEASE_BRANCH}" >/dev/null 2>&1; then
	if ! git fetch --no-tags origin "${RELEASE_BRANCH}"; then
		echo "::error::Could not fetch the existing downstream ${RELEASE_BRANCH} to inspect it; re-run once the remote is reachable."
		exit 1
	fi
	if tracks_engine_branch; then
		echo "Downstream ${RELEASE_BRANCH} already exists and tracks the engine ${RELEASE_BRANCH}; nothing to do."
		exit 0
	fi
	echo "::error::Downstream ${RELEASE_BRANCH} already exists and does not track the engine ${RELEASE_BRANCH}; refusing to overwrite it."
	exit 1
fi

git checkout -b "${RELEASE_BRANCH}" "${BASE_SHA}"

# Retarget every engine dependency onto the release branch.
#
# Delegated to the downstream repo's own dependency tooling when it supports
# branch pins (docker/patch-ce-local.py --pin-branch): the same parser and
# discovery the image build, the compat check and the release tag use, so the
# cut retargets EXACTLY the set of engine dependencies they operate on,
# however the manifests are formatted. Support is detected by flag presence,
# not script existence: downstream commits can carry revisions of the script
# that lack --pin-branch.
#
# Falls back to a line-oriented rewrite (the same one, and the same reasoning,
# as the fallback in tag-downstream-release.sh): drop whatever selector the
# line carries, then add `branch = <release branch>` right after the git URL.
# Engine deps are single-line inline tables, comment lines and non-engine git
# deps never match, and the engine `owner/repo` contains a `/` so engine lines
# are addressed with a custom sed delimiter (@). The re-lock verification
# below guards BOTH paths.
if [[ -f docker/patch-ce-local.py ]] && grep -q -- '--pin-branch' docker/patch-ce-local.py; then
	python3 -m pip install --quiet tomlkit 2>/dev/null || true
	WORKSPACE_DIR=. python3 docker/patch-ce-local.py --pin-branch "${RELEASE_BRANCH}"
else
	mapfile -t toml_files < <(grep -rl --include='Cargo.toml' -e "$ENGINE_MATCH" . || true)
	if [[ "${#toml_files[@]}" -eq 0 ]]; then
		echo "::error::No Cargo.toml references the engine repo (${ENGINE_MATCH}); nothing to retarget (unexpected)."
		exit 1
	fi
	for f in "${toml_files[@]}"; do
		if ! grep -Eq "git[[:space:]]*=[[:space:]]*\"[^\"]*${ENGINE_MATCH}[^\"]*\"" "$f"; then
			continue
		fi
		sed -i -E "\\@${ENGINE_MATCH}@ s/,[[:space:]]*(branch|tag|rev)[[:space:]]*=[[:space:]]*\"[^\"]*\"//g" "$f"
		sed -i -E "\\@${ENGINE_MATCH}@ s/(branch|tag|rev)[[:space:]]*=[[:space:]]*\"[^\"]*\"[[:space:]]*,[[:space:]]*//g" "$f"
		sed -i -E "\\@${ENGINE_MATCH}@ s#(git[[:space:]]*=[[:space:]]*\"[^\"]*${ENGINE_MATCH}[^\"]*\")#\1, branch = \"${RELEASE_BRANCH}\"#" "$f"
		echo "Retargeted engine deps in ${f}:"
		grep -nE "git[[:space:]]*=.*${ENGINE_MATCH}" "$f" || true
	done
fi

if [[ "$PUBLISH" != "true" ]]; then
	echo "[Dry-run] Manifest rewrite:"
	git --no-pager diff
	echo "[Dry-run] Would re-lock the engine crates against ${RELEASE_BRANCH} (skipped: the engine branch is not pushed on a dry-run) and push downstream ${RELEASE_BRANCH}."
	exit 0
fi

# Re-lock ONLY the engine crates (same discovery as tag-downstream-release.sh):
# they all come from a single git source, so updating them moves that source to
# the release branch while leaving every other dependency byte-identical to the
# committed lock.
mapfile -t engine_pkgs < <(awk -v m="${ENGINE_MATCH}" '
	/^name = /   { n = $0; sub(/^name = "/, "", n); sub(/"$/, "", n) }
	/^source = / && index($0, m) { print n }
' Cargo.lock | sort -u)
if [[ "${#engine_pkgs[@]}" -eq 0 ]]; then
	echo "::error::No engine crates found in Cargo.lock (no source matching '${ENGINE_MATCH}')."
	exit 1
fi
echo "Re-locking ${#engine_pkgs[@]} engine crate(s): ${engine_pkgs[*]}"
update_args=()
for p in "${engine_pkgs[@]}"; do
	update_args+=(-p "$p")
done
cargo update "${update_args[@]}"

# Verify every engine crate now resolves from the release branch (the lock
# records the branch URL-encoded in the source query, e.g. ?branch=releases%2F3.4).
enc_branch="${RELEASE_BRANCH//\//%2F}"
bad=$(awk -v m="${ENGINE_MATCH}" -v b="?branch=${enc_branch}#" '
	/^name = /   { n = $0; sub(/^name = "/, "", n); sub(/"$/, "", n) }
	/^source = / && index($0, m) && !index($0, b) { print n }
' Cargo.lock | sort -u)
if [[ -n "$bad" ]]; then
	printf '%s\n' "$bad"
	echo "::error::The engine crates above are not locked to ${RELEASE_BRANCH}; refusing to push."
	exit 1
fi
echo "Verified: all engine crates in Cargo.lock track ${RELEASE_BRANCH}."

git commit -am "chore: point engine deps at the ${RELEASE_BRANCH} branch"
git push origin "${RELEASE_BRANCH}"
echo "Pushed downstream ${RELEASE_BRANCH} (engine deps tracking ${RELEASE_BRANCH})."
