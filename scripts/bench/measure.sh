#!/usr/bin/env bash
#
# measure.sh — run a single language-test bench (optimised `profiling` profile)
# and print a machine-readable summary of the harness's statistics (time, median,
# and the comparison-vs-baseline change + p-value).
#
# Uses the `profiling` profile (release + thin-LTO) rather than full `--release`
# so it shares the binary built by profile.sh and rebuilds fast during the AI
# optimisation loop. Within ~1-3% of full-LTO release on hot loops.
#
# Usage:
#   scripts/bench/measure.sh <bench-filter> [--backend mem] [--save] [--quick] [--dataset NAME] [--out-dir DIR]
#
#   --save   Persist this run as the new baseline in the comparison datastore
#            (passes `--save` to `bench run`). Omit for a dry comparison against
#            the existing baseline.
#
#   --quick  Fast, coarse pass: shrinks every timing knob ~10x (passes `--quick`
#            to `bench run`). Catches large regressions, not small drift.
#
# The comparison block is produced by the harness itself
# (language-tests/src/cmd/bench/run.rs): it prints "Performance has improved" /
# "regressed" / "within noise threshold" / "No change in performance detected"
# plus a `change` interval and `p = …` line whenever a baseline exists. This
# script surfaces those lines verbatim and as a compact JSON object so an
# automated optimisation loop can decide accept/reject.
set -euo pipefail

# `cargo make bench-measure -- <args>` forwards the `--` separator through to
# this script as a literal first argument; drop it so the filter parses cleanly.
if [[ "${1:-}" == "--" ]]; then shift; fi

if [[ $# -lt 1 ]]; then
	echo "usage: $0 <bench-filter> [--backend mem] [--save] [--quick] [--dataset NAME] [--out-dir DIR]" >&2
	exit 2
fi

FILTER="$1"; shift
BACKEND="mem"
SAVE=""
QUICK=""
DATASET=()
OUT_DIR="./target/bench-profile"

while [[ $# -gt 0 ]]; do
	case "$1" in
		--backend) BACKEND="$2"; shift 2 ;;
		--save) SAVE="--save"; shift ;;
		--quick) QUICK="--quick"; shift ;;
		--dataset) DATASET=(--dataset "$2"); shift 2 ;;
		--out-dir) OUT_DIR="$2"; shift 2 ;;
		*) echo "unknown arg: $1" >&2; exit 2 ;;
	esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/language-tests"
mkdir -p "$ROOT/$OUT_DIR"
SLUG="$(echo "$FILTER" | tr '/ ' '__')"
LOG="$ROOT/$OUT_DIR/$SLUG.measure.txt"

# surrealkv is already in `bench`.
FEATURES="bench"

# shellcheck disable=SC2086
cargo run --profile profiling --features "$FEATURES" -- bench run --backend "$BACKEND" $SAVE $QUICK ${DATASET[@]+"${DATASET[@]}"} "$FILTER" \
	2>&1 | tee "$LOG"

echo ""
echo "---- summary ----"
# Pull the verdict + the time/median/change lines straight from the harness output.
# Each grep may legitimately find nothing (e.g. no baseline yet on a first
# `--save` run); `|| true` stops set -e from killing the script on no match.
verdict="$(grep -E 'Performance has|No change in performance|within noise threshold' "$LOG" | tail -1 | sed 's/^[[:space:]]*//' || true)"
time_line="$(grep -E '^[[:space:]]+time[[:space:]]+:' "$LOG" | tail -1 | sed 's/^[[:space:]]*//' || true)"
median_line="$(grep -E '^[[:space:]]+median[[:space:]]+:' "$LOG" | tail -1 | sed 's/^[[:space:]]*//' || true)"
change_line="$(grep -E '^[[:space:]]+change[[:space:]]+:' "$LOG" | tail -1 | sed 's/^[[:space:]]*//' || true)"

printf '%s\n' "${verdict:-<no baseline to compare against>}"
printf '%s\n' "${time_line:-time: <none>}"
printf '%s\n' "${median_line:-median: <none>}"
[[ -n "$change_line" ]] && printf '%s\n' "$change_line"

# Emit a compact JSON line (raw strings; numeric parsing is left to the caller
# since the harness prints Rust Duration debug formatting, e.g. "1.2ms"/"850µs").
python3 - "$FILTER" "$BACKEND" "$verdict" "$time_line" "$median_line" "$change_line" <<'PY'
import json, sys
filt, backend, verdict, time_line, median_line, change_line = sys.argv[1:7]
print("JSON " + json.dumps({
    "bench": filt,
    "backend": backend,
    "verdict": verdict or None,
    "time": time_line or None,
    "median": median_line or None,
    "change": change_line or None,
}))
PY
