/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_numeric_ranges_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_btree_numeric_ranges.surql and its
_backward sibling are answered by: each range is the index scan's own access,
in the direction the order asks for, with no filter above it to discard what a
misplaced bound admits and no sort above it to repair the order a misplaced
bound breaks, and each count is answered from the index keys alone. Without
these an index the new binary cannot read would fall back to a table scan and
return the same rows.

`EXPLAIN` output is not stable across the 3.x line, so the upgrade target is
the current checkout; the seed axis sends every released version straight to
it."""

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: >0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: >=0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: <0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: <=0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: <0, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: <=0, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: >-1 <1.5f, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: num, index: num_n, access: >-1 <1.5f, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: num, index: num_n, access: = 1dec, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: num, condition: n >= 0]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: num, condition: n > -1 AND n < 1.5f]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: >0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: <=0, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: <=0, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: >=0, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: >-1 <1.5f, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: numu, index: numu_n, access: = 1f, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: numu, condition: n >= 0]
''''

*/
EXPLAIN SELECT VALUE n FROM num WHERE n > 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM num WHERE n >= 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM num WHERE n < 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM num WHERE n <= 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM num WHERE n < 0 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM num WHERE n <= 0 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM num WHERE n > -1 AND n < 1.5 ORDER BY n;
EXPLAIN SELECT VALUE n FROM num WHERE n > -1 AND n < 1.5 ORDER BY n DESC;
EXPLAIN SELECT VALUE id FROM num WHERE n = 1dec;
EXPLAIN SELECT count() FROM num WHERE n >= 0 GROUP ALL;
EXPLAIN SELECT count() FROM num WHERE n > -1 AND n < 1.5 GROUP ALL;
EXPLAIN SELECT VALUE n FROM numu WHERE n > 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM numu WHERE n <= 0 ORDER BY n;
EXPLAIN SELECT VALUE n FROM numu WHERE n <= 0 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM numu WHERE n >= 0 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM numu WHERE n > -1 AND n < 1.5 ORDER BY n DESC;
EXPLAIN SELECT VALUE id FROM numu WHERE n = 1f;
EXPLAIN SELECT count() FROM numu WHERE n >= 0 GROUP ALL;
