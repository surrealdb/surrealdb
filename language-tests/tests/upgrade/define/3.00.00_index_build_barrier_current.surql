/**
[env]
imports = ["upgrade/define/3.00.00_index_build_barrier_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = """
An index that silently stops being used falls back to a table scan and returns
the same rows, so the reads in 3.00.00_index_build_barrier.surql cannot tell a
working index from a missing one. This asserts that the index the old release
built, and one the reader builds over the old rows, both serve their reads. It
is gated to the current checkout because `EXPLAIN` output is not stable across
the 3.x line; the seed axis still sends every released version straight to it."""

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "ib_f")'''

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "ib_g")'''

*/
-- The index the old release built serves an equality read.
EXPLAIN SELECT id FROM ib WHERE f = 'a';
-- So does one the reader builds over the old rows.
DEFINE INDEX ib_g ON TABLE ib COLUMNS g;
EXPLAIN SELECT id FROM ib WHERE g = 'x';
