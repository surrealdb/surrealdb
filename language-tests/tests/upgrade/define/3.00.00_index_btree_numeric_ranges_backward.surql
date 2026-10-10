/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_numeric_ranges_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Releases 3.0.3 to 3.0.5 drop the entry at the inclusive upper bound of a
# backward UNIQUE scan.
version = ">=3.1.0"
reason = """
The reads of 3.00.00_index_btree_numeric_ranges.surql scanned backward: a
backward scan starts at the range's upper key bound, so an inclusive upper
bound has to sort past every entry the old writer stored for that value, and
an exclusive one before them, in whichever kind each was written. The plans
are asserted in the _current sibling.

Numeric expectations compare by value, not kind, so ties between `1`, `1f` and
`1dec` are equal in whatever order they arrive."""

[[test.results]]
value = "[100000000000000000000dec, 9007199254740993, 9007199254740992, 2, 1.5, 1, 1, 1, 0.25, 0, 0, 0]"

[[test.results]]
value = "[-1, -2.5, -1000.5, -9007199254740993]"

[[test.results]]
value = "[0, 0, 0, -1, -2.5, -1000.5, -9007199254740993]"

[[test.results]]
value = "[1, 1, 1, 0.25, 0, 0, 0]"

[[test.results]]
value = "[100000000000000000000dec, 9007199254740993, 9007199254740992, 2, 1.5, 1, 0.25, 0]"

[[test.results]]
value = "[-1, -2.5, -1000.5, -9007199254740993]"

[[test.results]]
value = "[0, -1, -2.5, -1000.5, -9007199254740993]"

[[test.results]]
value = "[1, 0.25, 0]"

*/
-- Non-unique.
SELECT VALUE n FROM num WHERE n >= 0 ORDER BY n DESC;
SELECT VALUE n FROM num WHERE n < 0 ORDER BY n DESC;
SELECT VALUE n FROM num WHERE n <= 0 ORDER BY n DESC;
SELECT VALUE n FROM num WHERE n > -1 AND n < 1.5 ORDER BY n DESC;
-- Unique.
SELECT VALUE n FROM numu WHERE n >= 0 ORDER BY n DESC;
SELECT VALUE n FROM numu WHERE n < 0 ORDER BY n DESC;
SELECT VALUE n FROM numu WHERE n <= 0 ORDER BY n DESC;
SELECT VALUE n FROM numu WHERE n > -1 AND n < 1.5 ORDER BY n DESC;
