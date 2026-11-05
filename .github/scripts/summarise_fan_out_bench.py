#!/usr/bin/env python3
"""Turn a criterion run of `benches/fan_out.rs` into a comparison table.

The bench runs each query shape twice, as `overlapped` and `sequential`, which
criterion stores side by side under `target/criterion/fan_out/<shape>/<arm>/`.
This reads the point estimates back out and reports the ratio, so the answer to
"does overlapping the rows of a batch pay on this backend" is one column rather
than a wall of criterion output.

    summarise_fan_out_bench.py --backend kv-mem [--criterion-dir target/criterion]
"""

import argparse
import json
import sys
from pathlib import Path

ARMS = ("overlapped", "sequential")


def point_estimate(shape_dir: Path, arm: str) -> float | None:
    """Median nanoseconds for one arm, or None if criterion did not record it."""
    estimates = shape_dir / arm / "new" / "estimates.json"
    if not estimates.is_file():
        return None
    with estimates.open() as f:
        return json.load(f)["median"]["point_estimate"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", required=True, help="storage backend the run used")
    parser.add_argument("--criterion-dir", default="target/criterion", type=Path)
    args = parser.parse_args()

    group = args.criterion_dir / "fan_out"
    if not group.is_dir():
        print(f"No criterion output under {group}", file=sys.stderr)
        return 1

    rows = []
    for shape_dir in sorted(p for p in group.iterdir() if p.is_dir()):
        measured = {arm: point_estimate(shape_dir, arm) for arm in ARMS}
        if all(v is None for v in measured.values()):
            # Criterion keeps its own bookkeeping beside the shapes (`report`,
            # and one directory per parameter value for cross-shape comparison).
            # Those hold neither arm, so they are not a shape at all.
            continue
        if any(v is None for v in measured.values()):
            # Report the gap rather than dropping the shape: a missing arm means
            # the run did not measure what this table claims it did.
            rows.append((shape_dir.name, measured["overlapped"], measured["sequential"], None))
            continue
        rows.append(
            (
                shape_dir.name,
                measured["overlapped"],
                measured["sequential"],
                measured["sequential"] / measured["overlapped"],
            )
        )

    if not rows:
        print(f"No benchmark shapes found under {group}", file=sys.stderr)
        return 1

    def ms(value: float | None) -> str:
        return "n/a" if value is None else f"{value / 1e6:.3f} ms"

    print(f"### Fan-out overlap on `{args.backend}`")
    print()
    print("| shape | overlapped | sequential | ratio |")
    print("|---|---|---|---|")
    for name, overlapped, sequential, ratio in rows:
        verdict = "n/a" if ratio is None else f"{ratio:.2f}x"
        print(f"| `{name}` | {ms(overlapped)} | {ms(sequential)} | {verdict} |")
    print()
    print("A ratio above 1 means overlapping the rows of a batch was faster.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
