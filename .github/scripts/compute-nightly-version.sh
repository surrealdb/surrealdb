#!/usr/bin/env bash
set -e

CARGO_VERSION="$1"

if [[ -z "$CARGO_VERSION" ]]; then
	echo "Error: Cargo version argument required"
	echo "Usage: $0 <cargo-version>"
	exit 1
fi

# Nightly release - compute version based on main's version
major=$(echo $CARGO_VERSION | tr "." "\n" | sed -n 1p)
minor=$(echo $CARGO_VERSION | tr "." "\n" | sed -n 2p)
patch_full=$(echo $CARGO_VERSION | tr "." "\n" | sed -n 3p)

# If main has a pre-release version, use X.Y.Z-nightly
# If main is stable, use next minor with -nightly
if [[ "$CARGO_VERSION" =~ - ]]; then
	# Pre-release on main (e.g., 3.0.0-beta) -> 3.0.0-nightly
	# Extract just the numeric patch part (e.g., "0-beta" -> "0")
	patch=$(echo $patch_full | tr "-" "\n" | sed -n 1p)
	nightly_version="${major}.${minor}.${patch}-nightly"
else
	# Stable on main (e.g., 3.0.0) -> 3.1.0-nightly
	next_minor=$((minor + 1))
	nightly_version="${major}.${next_minor}.0-nightly"
fi

echo "$nightly_version"

