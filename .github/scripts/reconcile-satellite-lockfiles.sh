#!/usr/bin/env bash
set -euo pipefail

# Reconcile the lockfiles of every workspace outside the root one.
#
# `cargo set-version` rewrites the engine crates' versions across the root
# workspace, and every cargo invocation in the bump scripts sees only that
# workspace's own lockfile. The satellite workspaces — the language-test
# harness and the fuzz targets — depend on those crates by path, so a bump
# leaves each of their lockfiles naming a version that no longer exists. The
# next person to build one gets the reconciliation as an unrelated diff in
# their working tree, which then either rides along in their PR or has to be
# reverted by hand.
#
# `cargo metadata` performs exactly the reconciliation the manifests force and
# nothing more: a lockfile entry cargo is not obliged to move stays where it
# is. That is the discipline `cut-release.sh` states for the root lockfile —
# a deliberate dependency upgrade is its own PR, never a side effect of a
# version bump — and it is why this resolves rather than regenerating.
#
# The set is derived from where the lockfiles actually are, so a workspace
# added later is covered without editing this. A workspace with no lockfile of
# its own has nothing to drift and is correctly absent. `.ci` is skipped: the
# release workflow checks this tooling out there, inside the tree it is acting
# on, and that checkout is not what any bump is rewriting.

repo_root=$(git rev-parse --show-toplevel)
cd "${repo_root}"

found=0
while IFS= read -r lockfile; do
	manifest="${lockfile%Cargo.lock}Cargo.toml"
	if [[ ! -f "${manifest}" ]]; then
		echo "::warning::${lockfile} has no sibling Cargo.toml; skipping"
		continue
	fi
	echo "Reconciling ${lockfile}"
	cargo metadata --format-version 1 --manifest-path "${manifest}" >/dev/null
	found=$((found + 1))
done < <(find . -name Cargo.lock -not -path './Cargo.lock' -not -path '*/target/*' \
	-not -path './.git/*' -not -path './.ci/*' | sort)

echo "Reconciled ${found} satellite lockfile(s)"
