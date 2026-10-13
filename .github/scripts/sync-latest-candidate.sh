#!/usr/bin/env bash
set -euo pipefail

# Chooses the tag the public `latest` release pointer should move to for a
# bounded release sync, so the plan preview and the push reconciler cannot
# disagree: both call this. stdin is the NON-prerelease source tags within the
# bound (one per line; GitHub forbids pointing `latest` at a draft or
# prerelease, so prerelease tags must not be fed in); $1 is the destination's
# current latest tag (empty when it has none). Prints the newest input tag
# when the pointer may move to it, and nothing when the pointer must not move
# - the newest candidate below the destination's current latest is never
# promoted, so a backport sync cannot demote a newer public release.
#
# sort -V is safe here because the input carries no pre-release suffixes (the
# case -V mis-orders).

dst_latest="${1:-}"

candidate="$(grep . | sort -V | tail -1 || true)"
if [[ -z "$candidate" ]]; then
	exit 0
fi

if [[ -z "$dst_latest" ]]; then
	printf '%s\n' "$candidate"
	exit 0
fi

# The comparison runs in its own assignment - not inside a [[ ]] condition -
# so a tooling failure (missing python, unresolvable sibling script) fails the
# caller loudly instead of reading as a permanent "do not promote".
dst_le_candidate="$(printf '%s\n' "$dst_latest" \
	| python3 "$(dirname "${BASH_SOURCE[0]}")/release-tags-le.py" "$candidate")"
if [[ -n "$dst_le_candidate" ]]; then
	printf '%s\n' "$candidate"
fi
