/**
[env]
imports = ["upgrade/define/3.00.00_index_numeric_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = """
A numeric index that silently stops being used falls back to a table scan and
returns the same rows, so the lookups in 3.00.00_index_numeric.surql cannot
tell a working index from a missing one. This asserts that the index serves
both an equality and a range read. It is gated to the current checkout because
`EXPLAIN` output is not stable across the 3.x line; the seed axis still sends
every released version straight to it."""

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "idx_num_v")'''

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "idx_num_v")'''

*/
-- The index serves an equality read on a float key.
EXPLAIN SELECT id FROM idx_num WHERE v = 3.244745644034682e-48;
-- And a range read across signs.
EXPLAIN SELECT id FROM idx_num WHERE v > -10 AND v < 2;
