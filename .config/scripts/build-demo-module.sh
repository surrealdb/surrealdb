#!/usr/bin/env bash
# Nextest setup script: build the surrealism demo module once per test run.
#
# `tests/surrealism_integration.rs` needs `demo.surli`, a wasm32-wasip2 build
# of `surrealism/demo`. Nextest runs every test in its own process, so without
# this script each test would build the module itself, all at once.
#
# Contract: on success, exports `SURREALISM_DEMO_SURLI` (absolute path of the
# built archive) through `$NEXTEST_ENV`. On any failure it exports no archive
# and exits 0, and each test then falls back to building the module in-process,
# so the run is slower but still correct. Whenever it finds or builds
# `surreal-ext` it also exports `SURREAL_EXT_BIN`, which that fallback uses.
# The archive is read-only input for the tests; they copy it before modifying
# anything.
set -u

warn() { echo "build-demo-module: $*" >&2; }

root="${NEXTEST_WORKSPACE_ROOT:-$PWD}"

# `surreal-ext`, the extension tooling, packs the demo. It belongs beside the
# `surreal` binary the tests were built against, in the cargo profile directory,
# which nextest puts on the dynamic library search path. That entry is not the
# first one: build-script link-search paths precede it. Under
# `cargo nextest run --archive-file` the profile directory is the extracted
# archive, so `$root/target/debug` is only a fallback for in-place runs.
# `surreal-ext` is another package's binary, so it is there only when the run
# built (or archived) that package too; otherwise it is built here, into that
# directory, with the profile the tests were built with.
bin="${SURREAL_EXT_BIN:-}"
if [ -z "$bin" ]; then
	profile_dir=""
	libpath="${NEXTEST_DYLD_FALLBACK_LIBRARY_PATH:-${NEXTEST_LD_LIBRARY_PATH:-${NEXTEST_DYLD_LIBRARY_PATH:-}}}"
	IFS=: read -r -a libdirs <<< "$libpath"
	for dir in ${libdirs[@]+"${libdirs[@]}"} "$root/target/debug"; do
		if [ -n "$dir" ] && [ -f "$dir/surreal" ] && [ -x "$dir/surreal" ]; then
			profile_dir="$dir"
			break
		fi
	done
	if [ -z "$profile_dir" ]; then
		warn "no surreal binary found; tests will build the demo module themselves"
		exit 0
	fi
	bin="$profile_dir/surreal-ext"
	if [ ! -x "$bin" ]; then
		profile="$(basename "$profile_dir")"
		[ "$profile" = debug ] && profile=dev
		args=(build --locked --package surreal-ext --bin surreal-ext --profile "$profile")
		# `<target-dir>/<profile>` or `<target-dir>/<triple>/<profile>`; cargo
		# marks the target directory with CACHEDIR.TAG.
		parent="$(dirname "$profile_dir")"
		if [ -f "$parent/CACHEDIR.TAG" ]; then
			args+=(--target-dir "$parent")
		elif [ -f "$(dirname "$parent")/CACHEDIR.TAG" ]; then
			args+=(--target-dir "$(dirname "$parent")" --target "$(basename "$parent")")
		fi
		(cd "$root" && cargo "${args[@]}") >&2 || warn "cargo build of surreal-ext failed"
	fi
fi
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
	warn "no surreal-ext binary at $bin; tests will build the demo module themselves"
	exit 0
fi
# The tests pack the demo with the same binary if this script cannot.
echo "SURREAL_EXT_BIN=$bin" >> "$NEXTEST_ENV"

# Reuse an archive built alongside the nextest archive (placed at
# `target/demo.surli` by `test-build` in CI) so shards do not recompile the
# wasm32-wasip2 demo module.
if [ -s "$root/target/demo.surli" ]; then
	echo "SURREALISM_DEMO_SURLI=$(cd "$root/target" && pwd -P)/demo.surli" >> "$NEXTEST_ENV"
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

# No `--log warn`, as `surreal module build` needed: `surreal-ext` installs no
# logger, so an inherited `SURREAL_LOG` cannot flood the build.
if ! (cd "$root" && "$bin" build --debug -o "$out/demo.surli" "$root/surrealism/demo") >&2; then
	warn "demo module build failed; tests will build the demo module themselves"
	exit 0
fi
if [ ! -s "$out/demo.surli" ]; then
	warn "demo.surli missing after build; tests will build the demo module themselves"
	exit 0
fi

echo "SURREALISM_DEMO_SURLI=$out/demo.surli" >> "$NEXTEST_ENV"
