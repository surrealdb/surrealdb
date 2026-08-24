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
# Delegated to the downstream repo's own dependency tooling when present
# (docker/patch-ce-local.py --pin-rev): the same parser and discovery the
# rolling image build and the downstream-compat check use, so the tag pins
# EXACTLY the set of engine dependencies the build redirected - however many
# exist, however they are formatted or aliased, whatever URL spelling they
# use. tomlkit (best-effort install) keeps the committed pin diff surgical;
# without it the script re-serializes, which is semantically identical.
#
# Falls back to the legacy sed rewrite for downstream commits that predate
# the script (e.g. old release lines), whose manifests use the single-line
# form it understands. The lock verification in step 3 guards BOTH paths:
# any engine crate not pinned to the released sha refuses to tag.
#
# Existence of the script is a sufficient capability check: it was introduced
# complete with --pin-rev in a single downstream commit and backported intact
# to the release lines, so no downstream commit carries the script WITHOUT
# --pin-rev. A commit predating the script takes the sed fallback below.
if [[ -f docker/patch-ce-local.py ]]; then
	python3 -m pip install --quiet tomlkit 2>/dev/null || true
	WORKSPACE_DIR=. python3 docker/patch-ce-local.py --pin-rev "${ENGINE_SHA}"
else
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
fi

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

# The downstream commit the release was built from (HEAD before the pin
# commit): the equivalence check in step 5 compares an existing tag against
# it, so an overwrite re-release built from a DIFFERENT downstream commit is
# recognized as needing a new tag even when the engine sha is unchanged.
DOWNSTREAM_BASE="$(git rev-parse HEAD)"

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

# Does the existing remote tag match THIS release: annotation pins this engine
# sha, and the tagged commit sits on this downstream base (the base itself when
# nothing needed pinning, or a pin commit whose parent is the base). Both
# checks are needed - the annotation alone misses the same engine re-paired
# with a different downstream commit; ancestry alone misses a different engine
# pinned on the same base. Fetches the tag into refs/tags/existing-remote-tag.
existing_tag_matches() {
	git fetch --no-tags --force origin "refs/tags/${TAG}:refs/tags/existing-remote-tag"
	local commit parent
	commit="$(git rev-parse 'existing-remote-tag^{commit}' 2>/dev/null || echo '')"
	parent="$(git rev-parse 'existing-remote-tag^{commit}^' 2>/dev/null || echo '')"
	git tag -l --format='%(contents)' existing-remote-tag | grep -q "engine ${ENGINE_SHA}" \
		&& { [[ "$commit" == "$DOWNSTREAM_BASE" ]] || [[ "$parent" == "$DOWNSTREAM_BASE" ]]; }
}

if [[ "$tag_exists" == "true" && "$OVERWRITE" != "true" ]]; then
	# A resume legitimately re-runs this job with the tag already pushed, so an
	# existing MATCHING tag passes. A mismatched tag (a different engine, or
	# the same engine re-paired with a different downstream commit between
	# attempts) misrepresents the shipped image and must not pass silently on
	# the no-overwrite path - replacing it is an explicit operator decision.
	if existing_tag_matches; then
		echo "::notice::Downstream tag ${TAG} already pins engine ${ENGINE_SHA} on downstream ${DOWNSTREAM_BASE}; leaving it in place."
		exit 0
	fi
	echo "::error::Downstream tag ${TAG} exists and does not match this release (engine ${ENGINE_SHA} on downstream ${DOWNSTREAM_BASE}). Re-run with overwrite: true to replace it."
	exit 1
fi

git tag -f -a "${TAG}" -m "SurrealDB downstream release ${TAG}

Built against engine ${ENGINE_SHA}. The engine dependency is pinned to that
commit so this tag reproduces the code shipped in the released image."

if [[ "$PUBLISH" != "true" ]]; then
	echo "[dry-run] Pinned + tagged locally. Would push tag ${TAG} (engine ${ENGINE_SHA}) to the downstream repo."
	exit 0
fi

if [[ "$tag_exists" == "true" ]]; then
	# The downstream repo protects release tags against updates, so a
	# force-push may be rejected outright - and a re-run of the same release
	# does not need one: a matching tag is correct as-is (the pin commit
	# differs only in hash/timestamp). Only a mismatched tag actually needs
	# replacing.
	if existing_tag_matches; then
		echo "::notice::Downstream tag ${TAG} already pins engine ${ENGINE_SHA} on downstream ${DOWNSTREAM_BASE}; leaving the existing tag in place."
	else
		echo "Overwriting existing downstream tag ${TAG} (it pins a different engine commit or downstream base)."
		if ! git push --force origin "refs/tags/${TAG}"; then
			echo "::error::Downstream tag ${TAG} does not match this release (engine ${ENGINE_SHA} on downstream ${DOWNSTREAM_BASE}) and could not be replaced (the downstream repository protects tags against updates). Delete the tag on the downstream repo (or lift the protection for it) and re-run."
			exit 1
		fi
		echo "Pushed downstream tag ${TAG} (engine ${ENGINE_SHA})."
	fi
else
	git push origin "refs/tags/${TAG}"
	echo "Pushed downstream tag ${TAG} (engine ${ENGINE_SHA})."
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	echo "tag=${TAG}" >> "$GITHUB_OUTPUT"
fi
