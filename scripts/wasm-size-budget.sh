#!/usr/bin/env bash
#
# wasm-size-budget.sh — build the engine as an edge worker bundles it and fail
# when a bundle grows past its budget.
#
#   scripts/wasm-size-budget.sh [--toolchain stable|nightly] [--variant lean|full|edge|all]
#   scripts/wasm-size-budget.sh [--toolchain stable|nightly] --features <list>
#
# Each variant is `surrealdb-edge-size-probe` built for wasm32-unknown-unknown
# with the `edge` profile, passed through `wasm-bindgen` (as a worker build
# does; it strips the `__wasm_bindgen_unstable` section the JS bindings of the
# engine's dependencies leave) and then `wasm-opt -Oz`. The budget is on that
# output, which is what a worker uploads.
#
#   lean   the in-memory store and none of core's optional features: no
#          `regex-unicode`, `collate`, `geo`, `vector-index`, `fulltext`,
#          `observability`, `maintenance-tasks`, `text-diff`, `scram`, nor the
#          `api`, `files`, `search`, `encoding`, `url`, crypto and HTML function
#          families, nor the `shaped` result format.
#   full   every optional engine feature that builds for wasm32 (the probe's
#          `full` feature).
#   edge   the lean feature set and the `shaped` result format, entered through `Datastore::execute_edge` on a
#          datastore built by `Builder::for_edge` (the probe's `edge-entry`
#          feature): data statements and the edge builtins alone, with no
#          schema, `INFO`, live-query or sequence runtime, no index build and
#          no `http`, `eval`, `sleep` or password-hash functions.
#
# `--features` measures the lean bundle with the listed `surrealdb-core`
# features added (comma-separated), to price one feature against the lean
# bundle. It reports the size and checks no budget.
#
# Toolchains:
#
#   stable   the repository toolchain and the `edge` profile alone.
#   nightly  the toolchain pinned in `rust-toolchain.nightly` (installed, with
#            `rust-src`), which also rebuilds std and passes the size flags
#            below. This is the Cloudflare build.
#
# The nightly flags, each worth its place on the lean bundle:
#
#   -Zbuild-std=std,panic_abort     std compiled with the bundle's own settings
#   -Zbuild-std-features=optimize_for_size
#                                   std's size-optimised paths, and none of its
#                                   default features: no backtrace symbolisation
#                                   or symbol demangling
#   -Cpanic=immediate-abort         a panic traps at once: no formatting of the
#                                   panic message, no unwinding machinery
#   -Zlocation-detail=none          panic and `#[track_caller]` locations dropped
#   -Zfmt-debug=shallow             a derived `Debug` prints the type name only.
#                                   Error messages built with `{:?}` lose their
#                                   field values; `none` would also blank them
#                                   and is not used.
#
# `wasm-opt -Oz` runs with two more flags on both toolchains:
#
#   --low-memory-unused             rustc lays the shadow stack out first, so no
#                                   static or heap address lies below 1 KiB; the
#                                   flag lets wasm-opt fold small positive
#                                   constants into load/store offsets. It can only
#                                   turn an access that would wrap past 2^32 into
#                                   a trap.
#   --converge                      repeat the passes until the module stops
#                                   shrinking
#
# Both toolchains add `simd128` (Workers' V8 runs wasm SIMD) to what rustc
# already enables for wasm32-unknown-unknown — bulk-memory, sign-ext,
# nontrapping-fptoint, mutable-globals, multivalue and reference-types — and
# `wasm-opt` is passed the same set. `opt-level = 's'` builds larger than 'z'.
#
# Budgets, in bytes, override with the environment:
#
#   WASM_BUDGET_STABLE_LEAN, WASM_BUDGET_STABLE_FULL, WASM_BUDGET_STABLE_EDGE,
#   WASM_BUDGET_NIGHTLY_LEAN, WASM_BUDGET_NIGHTLY_FULL, WASM_BUDGET_NIGHTLY_EDGE
#
# A budget of 0 reports the size and checks nothing.
#
# Other environment: WASM_OPT (the `wasm-opt` binary, version 116 or later),
# WASM_BINDGEN (the `wasm-bindgen` CLI, the version `Cargo.lock` pins),
# CC_wasm32_unknown_unknown and AR_wasm32_unknown_unknown (a clang that targets
# wasm32; on macOS Homebrew's LLVM is used when present, as Apple's clang
# cannot).
set -euo pipefail

# `cargo make <task> -- <args>` forwards the `--` separator as an argument.
if [[ "${1:-}" == "--" ]]; then shift; fi

toolchain=stable
variant=all
custom_features=
while [[ $# -gt 0 ]]; do
	case "$1" in
	--toolchain) toolchain="$2"; shift 2 ;;
	--variant) variant="$2"; shift 2 ;;
	--features) variant=custom; custom_features="$2"; shift 2 ;;
	-h | --help)
		sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed -e '$d' -e 's/^# \{0,1\}//'
		exit 0
		;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

case "$variant" in
lean | full | edge | custom) variants=("$variant") ;;
all) variants=(lean full edge) ;;
*) echo "--variant must be lean, full, edge or all" >&2; exit 2 ;;
esac

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

# The budgets: the sizes measured when each was set, plus about 2% headroom.
budget_stable_lean=${WASM_BUDGET_STABLE_LEAN:-10627000}
budget_stable_full=${WASM_BUDGET_STABLE_FULL:-14489000}
budget_nightly_lean=${WASM_BUDGET_NIGHTLY_LEAN:-10000000}
budget_nightly_full=${WASM_BUDGET_NIGHTLY_FULL:-13228000}
budget_stable_edge=${WASM_BUDGET_STABLE_EDGE:-0}
budget_nightly_edge=${WASM_BUDGET_NIGHTLY_EDGE:-8350000}

wasm_opt=${WASM_OPT:-wasm-opt}
if ! command -v "$wasm_opt" >/dev/null; then
	echo "::error::wasm-opt not found; install binaryen or set WASM_OPT" >&2
	exit 1
fi
wasm_bindgen=${WASM_BINDGEN:-wasm-bindgen}
if ! command -v "$wasm_bindgen" >/dev/null; then
	echo "::error::wasm-bindgen not found; install wasm-bindgen-cli or set WASM_BINDGEN" >&2
	exit 1
fi

if [[ -z "${CC_wasm32_unknown_unknown:-}" && -x /opt/homebrew/opt/llvm/bin/clang ]]; then
	export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
	export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
fi

# `RUSTFLAGS` replaces `.cargo/config.toml`'s target rustflags rather than
# extending them, so the getrandom backend cfg and the 4 MiB stack (the
# parser's recursion at its depth limits needs most of the 1 MiB default) are
# carried here.
rustflags='--cfg getrandom_backend="wasm_js" -C link-arg=-zstack-size=4194304 -C target-feature=+simd128'
cargo_args=(build --locked -p surrealdb-edge-size-probe --target wasm32-unknown-unknown --profile edge)
case "$toolchain" in
stable) cargo=(cargo) ;;
nightly)
	cargo=(cargo "+$(tr -d '[:space:]' <rust-toolchain.nightly)")
	cargo_args+=(-Zbuild-std=std,panic_abort -Zbuild-std-features=optimize_for_size)
	rustflags+=" -Zunstable-options -Cpanic=immediate-abort -Zlocation-detail=none -Zfmt-debug=shallow"
	;;
*) echo "--toolchain must be stable or nightly" >&2; exit 2 ;;
esac

target_dir="$repo_root/target/wasm-size/$toolchain"
failed=0
printf '%-8s %-6s %12s %12s %12s\n' toolchain bundle size budget gzip
for v in "${variants[@]}"; do
	features=()
	[[ "$v" == full ]] && features=(--features full)
	[[ "$v" == edge ]] && features=(--features edge-entry)
	[[ "$v" == custom ]] && features=(--features "$(sed -E 's/([^,]+)/surrealdb-core\/\1/g' <<<"$custom_features")")
	RUSTFLAGS="$rustflags" CARGO_TARGET_DIR="$target_dir" \
		"${cargo[@]}" "${cargo_args[@]}" ${features[@]+"${features[@]}"} >&2
	raw="$target_dir/wasm32-unknown-unknown/edge/surrealdb_edge_size_probe.wasm"
	out="$target_dir/surrealdb_edge_size_probe.$v.wasm"
	rm -rf "$target_dir/bindgen"
	"$wasm_bindgen" --target web --out-dir "$target_dir/bindgen" "$raw"
	bindgen_out=$(find "$target_dir/bindgen" -name '*.wasm' ! -name '*.d.ts' | head -n 1)
	"$wasm_opt" -Oz --low-memory-unused --converge \
		--enable-bulk-memory --enable-sign-ext --enable-nontrapping-float-to-int \
		--enable-mutable-globals --enable-multivalue --enable-reference-types --enable-simd \
		--strip-debug --strip-producers "$bindgen_out" -o "$out"
	size=$(wc -c <"$out" | tr -d ' ')
	gz=$(gzip -9c "$out" | wc -c | tr -d ' ')
	if [[ "$v" == custom ]]; then
		printf '%-8s %-6s %12d %12s %12d  %s\n' "$toolchain" "$v" "$size" - "$gz" "$custom_features"
		continue
	fi
	budget_var="budget_${toolchain}_${v}"
	budget=${!budget_var}
	if ((budget == 0)); then
		printf '%-8s %-6s %12d %12s %12d\n' "$toolchain" "$v" "$size" - "$gz"
		echo "note: no $v budget set" >&2
		continue
	fi
	printf '%-8s %-6s %12d %12d %12d\n' "$toolchain" "$v" "$size" "$budget" "$gz"
	if ((size > budget)); then
		echo "::error::the $toolchain $v wasm bundle is $size bytes, over its budget of $budget" >&2
		failed=1
	fi
done
exit $failed
