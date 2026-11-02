/**
[env]
imports = ["upgrade/define/3.00.00_field_temporal_retype_interval_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A `duration` an older binary wrote casts to an `interval` on the new binary as
`(0, 0, microseconds)`, a value finer than a microsecond is refused instead of
rounded, and a field retyped to `interval` takes a backfill with the cast. A
`duration` does not coerce to an `interval`, so the backfill is the migration
path."""

[[test.results]]
value = "['PT1H30M', 'PT1.25S', 'PT0.000005S']"

[[test.results]]
value = "['PT48H', NONE, 'PT0S', NONE]"

[[test.results]]
error = "Could not cast into `interval` using input `1ns`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `d` of `ri:1`: Expected `interval` but found `1h30m`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 'PT1H30M', id: ri:1, o: 'PT48H' }, { d: 'PT1.25S', id: ri:2, o: NONE }, { d: 'PT0.000005S', id: ri:3, o: 'PT0S' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1h30m, 1s250ms, 5µs]"
*/
-- The cast reads the stored durations.
SELECT VALUE <string><interval> d FROM ri WHERE id IN [ri:1, ri:2, ri:3] ORDER BY id;
SELECT VALUE <option<string>><option<interval>> o FROM ri ORDER BY id;
-- A sub-microsecond value is refused.
SELECT VALUE <interval> d FROM ri:4;
-- Retype the field; the stored durations are not touched.
DEFINE FIELD OVERWRITE d ON ri TYPE interval;
DEFINE FIELD OVERWRITE o ON ri TYPE option<interval>;
-- A duration does not coerce to the retyped field.
UPDATE ri:1 SET d = d;
-- Backfill the rows that fit, which lets the refused one be removed.
UPDATE ri:1, ri:2, ri:3 SET d = <interval> d, o = <option<interval>> o RETURN NONE;
SELECT id, <string> d AS d, <option<string>> o AS o FROM ri:1, ri:2, ri:3 ORDER BY id;
DELETE ri:4;
-- The round trip back to a duration is exact.
SELECT VALUE <duration> d FROM ri ORDER BY id;
