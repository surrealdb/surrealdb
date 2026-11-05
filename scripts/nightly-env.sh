#!/usr/bin/env bash
# Source this from cargo-nightly or setup-environment. Do not change global
# rustup defaults or the stable repository toolchain.
nightly_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export RUSTUP_TOOLCHAIN="${SURREAL_NIGHTLY_RUST_TOOLCHAIN:-$(cat "$nightly_root/rust-toolchain.nightly")}"
export SURREAL_RUSTC_THREADS="${SURREAL_RUSTC_THREADS:-8}"
if [[ ! "$SURREAL_RUSTC_THREADS" =~ ^[1-9][0-9]*$ ]]; then
    echo 'SURREAL_RUSTC_THREADS must be a positive integer' >&2
    return 1
fi
export SURREAL_NIGHTLY_RUSTFLAGS="-Zthreads=$SURREAL_RUSTC_THREADS"
# Cargo gives explicit environment flags precedence over target config. Append
# to those when present; otherwise cfg(all()) adds to target-specific flags,
# preserving the wasm getrandom backend and musl/Windows linker settings.
if [[ ${CARGO_ENCODED_RUSTFLAGS+x} ]]; then
    export CARGO_ENCODED_RUSTFLAGS="${CARGO_ENCODED_RUSTFLAGS}${CARGO_ENCODED_RUSTFLAGS:+$'\x1f'}$SURREAL_NIGHTLY_RUSTFLAGS"
elif [[ ${RUSTFLAGS+x} ]]; then
    export RUSTFLAGS="${RUSTFLAGS}${RUSTFLAGS:+ }$SURREAL_NIGHTLY_RUSTFLAGS"
fi
# Allow Cargo to generate crate docs concurrently and merge their indexes.
export CARGO_UNSTABLE_RUSTDOC_MERGEABLE_INFO=true
export RUSTDOCFLAGS="${RUSTDOCFLAGS:-}${RUSTDOCFLAGS:+ }$SURREAL_NIGHTLY_RUSTFLAGS"
nightly_cargo_config="target.'cfg(all())'.rustflags=[\"$SURREAL_NIGHTLY_RUSTFLAGS\"]"
