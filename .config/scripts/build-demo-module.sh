#!/usr/bin/env bash
# Nextest setup script: build the surrealism demo module once per test run.
#
# `tests/surrealism_integration.rs` needs `demo.surli`, a wasm32-wasip2 build
# of `surrealism/demo`. Nextest runs every test in its own process, so without
# this script each test would build the module itself, all at once.
#
# Contract: on success, exports `SURREALISM_DEMO_SURLI` (absolute path of the
# built archive) through `$NEXTEST_ENV`. On any failure it exports nothing and
# exits 0, and each test then falls back to building the module in-process, so
# the run is slower but still correct. The archive is read-only input for the
# tests; they copy it before modifying anything.
set -u

warn() { echo "build-demo-module: $*" >&2; }

root="${NEXTEST_WORKSPACE_ROOT:-$PWD}"

# The `surreal` binary the tests were built against sits in the cargo profile
# directory, which nextest puts on the dynamic library search path. That entry
# is not the first one: build-script link-search paths precede it. Under
# `cargo nextest run --archive-file` the profile directory is the extracted
# archive, so `$root/target` is only a fallback for in-place runs.
bin="${SURREAL_BIN:-}"
if [ -z "$bin" ]; then
	libpath="${NEXTEST_DYLD_FALLBACK_LIBRARY_PATH:-${NEXTEST_LD_LIBRARY_PATH:-${NEXTEST_DYLD_LIBRARY_PATH:-}}}"
	IFS=: read -r -a libdirs <<< "$libpath"
	for dir in ${libdirs[@]+"${libdirs[@]}"}; do
		if [ -n "$dir" ] && [ -f "$dir/surreal" ] && [ -x "$dir/surreal" ]; then
			bin="$dir/surreal"
			break
		fi
	done
	if [ -z "$bin" ] && [ -x "$root/target/debug/surreal" ]; then
		bin="$root/target/debug/surreal"
	fi
fi
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
	warn "no surreal binary found; tests will build the demo module themselves"
	exit 0
fi

# One directory per run, under the cargo target directory so stale ones are
# pruned rather than leaked into the system temp directory.
base="$(dirname "$bin")/surrealism-demo"
out="$base/${NEXTEST_RUN_ID:-local}"
mkdir -p "$base" && find "$base" -mindepth 1 -maxdepth 1 -type d -mtime +1 -exec rm -rf {} + 2>/dev/null
rm -rf "$out"
mkdir -p "$out" || { warn "cannot create $out"; exit 0; }
out="$(cd "$out" && pwd -P)"

# `--log warn` overrides an inherited `SURREAL_LOG`: cargo-make sets it to
# `full`, at which compiling the module logs every wasmtime and cranelift pass,
# hundreds of megabytes that also slow the build itself.
if ! (cd "$root" && "$bin" module build --log warn --debug -o "$out/demo.surli" "$root/surrealism/demo") >&2; then
	warn "demo module build failed; tests will build the demo module themselves"
	exit 0
fi
if [ ! -s "$out/demo.surli" ]; then
	warn "demo.surli missing after build; tests will build the demo module themselves"
	exit 0
fi

echo "SURREALISM_DEMO_SURLI=$out/demo.surli" >> "$NEXTEST_ENV"
