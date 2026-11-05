#!/usr/bin/env bash
set -euo pipefail

# Assert that every workspace's Cargo.lock already satisfies its manifests.
#
# `cargo metadata --locked` resolves the dependency graph and refuses to write
# the lockfile, so a lockfile that no longer satisfies its manifests exits
# non-zero here. Resolution only — nothing is compiled — so every workspace in
# the repository is covered in seconds.
#
# The satellite workspaces are why this is worth a job of its own. They reach
# the engine crates by path, so those crates' `workspace = true` dependency
# requirements still resolve against the root manifest, while each satellite
# carries its own lockfile. A dependency requirement changed in the root
# manifest therefore invalidates every satellite lockfile that reaches the
# crate, and updating the root lockfile alone leaves them behind.
#
# HAZARD, and the reason this job holds no cache step and no other cargo
# invocation: any cargo command that resolves *without* `--locked` rewrites the
# lockfile in place. Once that has happened on a runner, every later `--locked`
# check in the same job inspects the lockfile the runner just reconciled rather
# than the one in the commit, and passes unconditionally. `Swatinem/rust-cache`
# resolves each workspace it is handed, so a `--locked` assertion sequenced
# after the shared setup action is not an assertion about the commit. Add
# nothing to this job that resolves.
#
# The set is derived from where the lockfiles are, so a workspace added later
# is covered without editing this file. `.ci` is skipped: the release workflow
# checks its tooling out there, inside the tree it is acting on, and that
# checkout is not part of the commit under test.

repo_root=$(git rev-parse --show-toplevel)
cd "${repo_root}"

checked=0
stale=()

while IFS= read -r lockfile; do
	manifest="${lockfile%Cargo.lock}Cargo.toml"
	if [[ ! -f "${manifest}" ]]; then
		echo "::warning::${lockfile} has no sibling Cargo.toml; skipping"
		continue
	fi
	echo "Checking ${lockfile}"
	if cargo metadata --locked --format-version 1 --manifest-path "${manifest}" >/dev/null; then
		checked=$((checked + 1))
	else
		stale+=("${lockfile}")
	fi
done < <(find . -name Cargo.lock -not -path '*/target/*' -not -path './.git/*' \
	-not -path './.ci/*' | sort)

if ((${#stale[@]} > 0)); then
	for lockfile in "${stale[@]}"; do
		echo "::error file=${lockfile}::${lockfile} no longer satisfies its manifests. Regenerate it with: cargo metadata --format-version 1 --manifest-path ${lockfile%Cargo.lock}Cargo.toml"
	done
	echo
	echo "${#stale[@]} lockfile(s) out of date, ${checked} up to date."
	echo "A dependency requirement changed in the root manifest has to be resolved"
	echo "into every satellite workspace as well; .github/scripts/reconcile-satellite-lockfiles.sh"
	echo "does all of them at once."
	exit 1
fi

echo "All ${checked} lockfile(s) satisfy their manifests"
