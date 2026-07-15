#!/usr/bin/env bash
set -euo pipefail

# Confidentiality guard.
#
# Fails if any confidential string appears in a tracked file, so a leak is
# caught on the PR and never reaches a branch that is mirrored to the public
# engine repo. Because PRs are squash-merged, a fix stays out of the permanent
# history entirely.
#
# The denylist is assembled at runtime from repository secrets and variables
# and is NEVER hardcoded here (this script is itself mirrored, so a literal
# term would leak the very thing it guards): the sensitive coordinates plus the
# freeform, newline-separated CONFIDENTIAL_TERMS list (e.g. the private engine
# repo name, internal account IDs). With no terms available the guard fails
# closed rather than passing silently.
#
# Only values that are genuinely confidential AND specific enough not to occur
# in ordinary source are included. The bare downstream repo and ECR repository
# names are deliberately omitted: they are common words that appear throughout
# the tree, and the sensitive coordinates are covered anyway (the registry host
# carries the account ID and is matched; a specific full slug can be added to
# CONFIDENTIAL_TERMS), so nothing is lost by excluding them.

patterns="$(mktemp)"
matches="$(mktemp)"
trap 'rm -f "$patterns" "$matches"' EXIT

append() {
	if [ -n "${1:-}" ]; then
		printf '%s\n' "$1" >>"$patterns"
	fi
}

append "${AWS_ECR_REGISTRY:-}"
append "${DOWNSTREAM_APP_CLIENT_ID:-}"
if [ -n "${CONFIDENTIAL_TERMS:-}" ]; then
	printf '%s\n' "${CONFIDENTIAL_TERMS}" >>"$patterns"
fi

# Normalise: strip CRs (in case a value was pasted with CRLF) and drop
# blank/whitespace-only lines, which would otherwise match every file.
tr -d '\r' <"$patterns" | sed '/^[[:space:]]*$/d' >"${patterns}.clean"
mv "${patterns}.clean" "$patterns"

if [ ! -s "$patterns" ]; then
	echo "::error title=Confidentiality::No confidential terms available (DOWNSTREAM_* / CONFIDENTIAL_TERMS unset). Failing closed."
	exit 1
fi

# Case-insensitive, fixed-string search over every tracked file except this
# script. Only the matched substring is emitted (-o), as "path:line:match", to
# keep output focused instead of dumping whole lines. This workflow only ever
# runs on the private engine repo (the public mirror never runs it, so no
# secret is available there), and Actions masks any registered secret as *** in
# the logs regardless, so nothing extra is exposed.
set +e
git grep --no-color -o -n -I -F -i -f "$patterns" -- ':!.github/scripts/check-confidentiality.sh' >"$matches"
rc=$?
set -e

if [ "$rc" -eq 0 ]; then
	echo "::error title=Confidentiality::Confidential reference(s) found in tracked files. Move the value behind a secret/variable instead of committing it:"
	sort -u "$matches"
	exit 1
elif [ "$rc" -eq 1 ]; then
	echo "No confidential references found."
else
	echo "::error title=Confidentiality::git grep failed (exit ${rc})."
	exit "$rc"
fi
