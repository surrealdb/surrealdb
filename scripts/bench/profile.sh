#!/usr/bin/env bash
#
# profile.sh — profile ONLY the measured statements of a single language-test
# bench with `samply`, skipping the dataset import + warmup.
#
# How: the bench harness prints `__BENCH_MEASURE_START__` on stderr (when
# `BENCH_MARKERS=1`) just before the timed loop. This script launches the bench,
# waits for that marker — so the 100k-row import and warmup have finished — then
# attaches `samply record -p <pid>` for the measured region only. The resulting
# flamegraph is the query under test, not the setup.
#
# Why samply: it samples without `sudo` on macOS and Linux (`cargo flamegraph`
# uses `dtrace`, which is SIP-restricted on macOS).
#
# Usage:
#   scripts/bench/profile.sh <bench-filter> [--backend mem] [--quick] [--dataset NAME] [--out-dir DIR]
#
#   <bench-filter>  Substring matched against the bench file path. Must select
#                   exactly one bench. For a matrix scan that runs both dataset
#                   variants, pass `--dataset <name>` so there is a single import
#                   and a single measured region (otherwise the second variant's
#                   import would be sampled too).
#
# Output (default dir: language-tests/target/bench-profile):
#   <slug>.json.gz   samply profile — view with: samply load <file>
#
# Build profile: `profiling` (release + line-tables-only debug, unstripped,
# panic=unwind, thin-LTO). Requires `samply` (installed on first use).
#
# `profiling` inlines less than the `release` build the nightly measures, which
# keeps frames readable but means a cost can sit in a different function here
# than it does there. To attribute a measured bench delta rather than explore,
# build with `--profile bench-profiling`, which matches the measured codegen.
set -euo pipefail

# `cargo make bench-profile -- <args>` forwards the `--` separator through to
# this script as a literal first argument; drop it so the filter parses cleanly.
if [[ "${1:-}" == "--" ]]; then shift; fi

if [[ $# -lt 1 ]]; then
	echo "usage: $0 <bench-filter> [--backend mem] [--quick] [--dataset NAME] [--out-dir DIR]" >&2
	exit 2
fi

FILTER="$1"; shift
BACKEND="mem"
OUT_DIR="./target/bench-profile"
EXTRA=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		--backend) BACKEND="$2"; shift 2 ;;
		--out-dir) OUT_DIR="$2"; shift 2 ;;
		--quick) EXTRA+=(--quick); shift ;;
		--dataset) EXTRA+=(--dataset "$2"); shift 2 ;;
		*) echo "unknown arg: $1" >&2; exit 2 ;;
	esac
done

# Resolve repo root (this script lives in <root>/scripts/bench).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# `language-tests` is its own cargo workspace; build/run there.
cd "$ROOT/language-tests"

command -v samply >/dev/null 2>&1 || cargo install samply

mkdir -p "$OUT_DIR"
SLUG="$(echo "$FILTER" | tr '/ ' '__')"
PROFILE="$(cd "$OUT_DIR" && pwd)/$SLUG.json.gz"

# rocksdb needs its cargo feature; surrealkv is already in `bench`.
FEATURES="bench"; [ "$BACKEND" = "rocksdb" ] && FEATURES="bench,backend-rocksdb"

echo ">> building (profile=profiling, features=$FEATURES)"
cargo build --profile profiling --features "$FEATURES"

BIN="$ROOT/language-tests/target/profiling/surrealql-test"

# macOS: samply must be codesigned to sample / attach to processes (one-time).
if [[ "$(uname)" == "Darwin" ]]; then
	samply setup --yes >/dev/null 2>&1 || true
fi

STDERR_LOG="$(mktemp)"
trap 'rm -f "$STDERR_LOG"' EXIT

echo ">> launching bench (import + warmup happen now, unsampled)"
# BENCH_MARKERS makes the harness emit the measured-region sentinels on stderr.
BENCH_MARKERS=1 "$BIN" bench run --backend "$BACKEND" "$FILTER" ${EXTRA[@]+"${EXTRA[@]}"} 2>"$STDERR_LOG" &
PID=$!

echo ">> waiting for the measured-region marker (setup to finish)…"
while ! grep -q "__BENCH_MEASURE_START__" "$STDERR_LOG" 2>/dev/null; do
	if ! kill -0 "$PID" 2>/dev/null; then
		echo "!! bench exited before the measured region. stderr:" >&2
		cat "$STDERR_LOG" >&2
		echo "   (is the binary built from a branch with BENCH_MARKERS support?)" >&2
		wait "$PID" || true
		exit 1
	fi
	sleep 0.2
done

echo ">> attaching samply to PID $PID (sampling the measured statements)"
# In attach mode samply records until interrupted (it won't auto-exit when the
# target dies — it just prints "All tasks terminated" and waits). So run it in the
# background, wait for the bench to finish, then SIGINT samply to finalise + write.
#
# --unstable-presymbolicate resolves symbols now (while the binary and its .o
# debug-map files are present) and writes a <profile>.syms.json sidecar, so
# `samply load` shows function names instead of raw hex addresses. Attach mode
# otherwise defers symbolication to load time and can't find the debug info.
samply record --save-only --unstable-presymbolicate -o "$PROFILE" -p "$PID" &
SAMPLY_PID=$!

wait "$PID" || true
kill -INT "$SAMPLY_PID" 2>/dev/null || true
wait "$SAMPLY_PID" 2>/dev/null || true

echo ""
echo "profile saved : $PROFILE"
echo "view it with  : samply load \"$PROFILE\""
echo "                (the import/warmup are excluded — this is the query only)"
