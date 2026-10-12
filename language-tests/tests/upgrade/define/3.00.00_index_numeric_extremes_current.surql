/**
[env]
# The body asserts `EXPLAIN` plans, and the ordinary language-test runner
# replays it as well as the upgrade axis.
imports = ["upgrade/define/3.00.00_index_numeric_extremes_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = """
An index that silently stops being used falls back to a table scan and returns
the same rows, so the lookups in 3.00.00_index_numeric_extremes.surql cannot
tell a working index from a missing one. This asserts that the standard, UNIQUE
and compound indexes serve equality and range reads over keys the old binary
wrote. It is gated to the current checkout because `EXPLAIN` output is
not stable across the 3.x line; the seed axis still sends every released
version straight to it."""

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nx_v")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nx_v")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nxu_v")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nxc_ab")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nxc_ab")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "nxcu_ab")'''

*/
-- Equality on a float key that is not exactly representable.
EXPLAIN SELECT id FROM nx WHERE v = 0.3333333333333333f;
-- A range across both signs.
EXPLAIN SELECT id FROM nx WHERE v > -1e19 AND v < 1e19;
-- Equality on a UNIQUE key.
EXPLAIN SELECT id FROM nxu WHERE v = 79228162514264337593543950335dec;
-- Equality on both columns of a compound key, and on the first with a range
-- on the second.
EXPLAIN SELECT id FROM nxc WHERE a = 1 AND b = 0.5f;
EXPLAIN SELECT id FROM nxc WHERE a = 1 AND b < 1;
EXPLAIN SELECT id FROM nxcu WHERE a = 3 AND b = 3.486844328132695e67;
