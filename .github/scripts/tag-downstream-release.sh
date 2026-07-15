#!/usr/bin/env bash
set -euo pipefail

# Pin the downstream engine dependency to the exact released engine
# commit and create a REPRODUCIBLE tag, so an engineer can clone the downstream
# repo, `git checkout <tag>`, and rebuild the exact code that shipped in the
# released image.
#
# Why this is needed
# ------------------
# The released downstream image is built from a downstream commit with a Cargo
# `[patch]` that redirects the engine dependency (surrealdb-server et al.) to a
# COLOCATED checkout of this engine at the released engine sha. The downstream's
# own manifest, by contrast, tracks the engine repo by BRANCH (e.g. `main`), and
# its committed Cargo.lock pins whatever commit that branch happened to be at.
# So a plain `git checkout <downstream-sha>` fetches the WRONG engine code. To
# reproduce the release we must pin the engine dependency to the exact released
# engine sha before tagging.
#
# What it does
# ------------
#   1. Rewrite every engine dependency (any Cargo.toml referencing the engine
#      repo) from its branch/tag/rev selector to `rev = <engine-sha>`.
#   2. Re-lock ONLY the engine crates. Every other dependency is left exactly as
#      the committed downstream Cargo.lock had it — the same lock the image build
#      started from — so the tag reproduces the shipped dependency graph by
#      construction (no blanket `cargo update`).
#   3. Verify every engine crate in the lock is now pinned to <engine-sha>.
#   4. Commit that pin on top of <downstream-sha> and create an annotated tag.
#   5. Push ONLY the tag (never a branch — the pin commit exists solely to anchor
#      the tag). Idempotent: an existing tag is left untouched unless overwrite.
#
# This script runs INSIDE a checkout of the downstream repo (detached at the
# downstream sha). It relies on:
#   * `origin` being the downstream repo with push credentials already configured
#     (actions/checkout with a Contents:write token),
#   * cargo being able to fetch the engine repo over git (the caller configures a
#     token-authenticated `url.<...>.insteadOf` rewrite plus
#     CARGO_NET_GIT_FETCH_WITH_CLI=true so the engine's declared ssh URL resolves).
#
# Usage: tag-downstream-release.sh <tag> <engine-sha> [publish] [overwrite]
#   publish=false   -> dry-run: pin + validate, but do NOT push the tag.
#   overwrite=true  -> replace an already-existing tag (force). Otherwise an
#                      existing tag is left untouched.
#
# Requires the ENGINE_REPO env var: the engine's `owner/repo` (e.g. from the
# PRIVATE_ENGINE_REPO CI variable). It is matched as a literal substring to
# identify the engine crates, so the engine's identity comes from CI config
# rather than being hardcoded in this script (which is mirrored publicly).

TAG="${1:?tag name required (e.g. v3.2.0)}"
ENGINE_SHA="${2:?engine sha required}"
PUBLISH="${3:-false}"
OVERWRITE="${4:-false}"

# The engine's `owner/repo`. It appears in every engine git URL (Cargo.toml) and
# every engine `source` line (Cargo.lock), and in no other git dependency, so it
# reliably selects exactly the engine crates.
ENGINE_MATCH="${ENGINE_REPO:?ENGINE_REPO env var required (the engine owner/repo)}"

echo "Pinning downstream engine dependencies to ${ENGINE_SHA} for tag ${TAG}"

# ---------------------------------------------------------------------------
# 1) Rewrite every engine dependency to rev = <engine-sha>.
# ---------------------------------------------------------------------------
# Engine deps are declared as single-line inline tables, e.g.
#   <crate> = { git = "ssh://.../<engine-repo>.git", branch = "main" }
# Drop whatever version selector (branch/tag/rev) the line carries, then add
# `rev = <engine-sha>` right after the git URL. Any other keys on the line are
# preserved. Comment lines and non-engine git deps never match (they carry no
# engine `git = "..."` URL), so they are left untouched.
#
# The engine `owner/repo` contains a `/`, so engine lines are addressed with a
# custom sed delimiter (@) rather than the default `/`.
mapfile -t toml_files < <(grep -rl --include='Cargo.toml' -e "$ENGINE_MATCH" . || true)
if [[ "${#toml_files[@]}" -eq 0 ]]; then
	echo "::error::No Cargo.toml references the engine repo (${ENGINE_MATCH}); nothing to pin (unexpected)."
	exit 1
fi

for f in "${toml_files[@]}"; do
	# Only touch lines that actually declare an engine git dependency.
	if ! grep -Eq "git[[:space:]]*=[[:space:]]*\"[^\"]*${ENGINE_MATCH}[^\"]*\"" "$f"; then
		continue
	fi
	# Drop an existing branch/tag/rev selector, whether it trails or leads the
	# git key (handles `git=..., branch=...` and `branch=..., git=...`).
	sed -i -E "\\@${ENGINE_MATCH}@ s/,[[:space:]]*(branch|tag|rev)[[:space:]]*=[[:space:]]*\"[^\"]*\"//g" "$f"
	sed -i -E "\\@${ENGINE_MATCH}@ s/(branch|tag|rev)[[:space:]]*=[[:space:]]*\"[^\"]*\"[[:space:]]*,[[:space:]]*//g" "$f"
	# Add the exact rev right after the git URL.
	sed -i -E "\\@${ENGINE_MATCH}@ s#(git[[:space:]]*=[[:space:]]*\"[^\"]*${ENGINE_MATCH}[^\"]*\")#\1, rev = \"${ENGINE_SHA}\"#" "$f"
	echo "Rewrote engine deps in ${f}:"
	grep -nE "git[[:space:]]*=.*${ENGINE_MATCH}" "$f" || true
done

# ---------------------------------------------------------------------------
# 2) Re-lock ONLY the engine crates.
# ---------------------------------------------------------------------------
# Discover every crate Cargo has locked from the engine repo (a package block
# whose `source` line matches the engine), then refresh just those. Because the
# engine crates all come from a single git source, updating them moves the whole
# source to the new rev while leaving every non-engine dependency byte-identical
# to the committed lock.
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

# ---------------------------------------------------------------------------
# 3) Verify every engine crate is now pinned to exactly <engine-sha>.
# ---------------------------------------------------------------------------
# A locked git source ends with `#<commit>"`. Any engine source not ending in
# the released commit means the pin only partially applied — refuse to tag.
mapfile -t mispinned < <(grep -E '^source = "git\+[^"]*'"${ENGINE_MATCH}" Cargo.lock \
	| grep -vE "#${ENGINE_SHA}\"\$" || true)
if [[ "${#mispinned[@]}" -gt 0 ]]; then
	printf '  %s\n' "${mispinned[@]}"
	echo "::error::The engine crates above are not pinned to ${ENGINE_SHA}; refusing to tag."
	exit 1
fi
if ! grep -qE '^source = "git\+[^"]*'"${ENGINE_MATCH}"'[^"]*#'"${ENGINE_SHA}"'"$' Cargo.lock; then
	echo "::error::No engine crate is pinned to ${ENGINE_SHA} after the update; refusing to tag."
	exit 1
fi
echo "Verified: all engine crates in Cargo.lock are pinned to ${ENGINE_SHA}."

# ---------------------------------------------------------------------------
# 4) Commit the pin on top of the downstream sha.
# ---------------------------------------------------------------------------
git config user.name "surrealdb-release-bot"
git config user.email "release-bot@surrealdb.com"

git add -A
if git diff --cached --quiet; then
	# Already pinned (e.g. a retry after the commit but before the push). Tag the
	# current commit as-is rather than failing.
	echo "No changes after pinning; tagging the current commit."
else
	git commit -m "Pin engine to ${ENGINE_SHA} for ${TAG}"
fi

# ---------------------------------------------------------------------------
# 5) Create + push the tag (tag only, never a branch).
# ---------------------------------------------------------------------------
if git ls-remote --exit-code --tags origin "refs/tags/${TAG}" >/dev/null 2>&1; then
	tag_exists=true
else
	tag_exists=false
fi

if [[ "$tag_exists" == "true" && "$OVERWRITE" != "true" ]]; then
	echo "::notice::Downstream tag ${TAG} already exists; leaving it untouched (re-run with overwrite: true to replace it)."
	exit 0
fi

git tag -f -a "${TAG}" -m "SurrealDB downstream release ${TAG}

Built against engine ${ENGINE_SHA}. The engine dependency is pinned to that
commit so this tag reproduces the code shipped in the released image."

if [[ "$PUBLISH" != "true" ]]; then
	echo "[dry-run] Pinned + tagged locally. Would push tag ${TAG} (engine ${ENGINE_SHA}) to the downstream repo."
	exit 0
fi

if [[ "$tag_exists" == "true" ]]; then
	echo "Overwriting existing downstream tag ${TAG}."
	git push --force origin "refs/tags/${TAG}"
else
	git push origin "refs/tags/${TAG}"
fi
echo "Pushed downstream tag ${TAG} (engine ${ENGINE_SHA})."

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	echo "tag=${TAG}" >> "$GITHUB_OUTPUT"
fi
