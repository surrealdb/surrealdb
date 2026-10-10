/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_compound_ranges_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_btree_compound_ranges.surql are answered
by: each comparison on the column after the pinned prefix is the index scan's
own access, with no filter above it to discard what a misplaced bound admits.
Without these an index the new binary cannot read would fall back to a table
scan and return the same rows.

`EXPLAIN` output is not stable across the 3.x line, so the upgrade target is
the current checkout; the seed axis sends every released version straight to
it."""

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x', 1], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x'] LessThan 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x'] LessThanEqual 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x'] MoreThan 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x'] MoreThanEqual 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmp, index: cmp_ab, access: ['x'], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmpu, index: cmpu_ab, access: ['x', 1], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmpu, index: cmpu_ab, access: ['x'] LessThan 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmpu, index: cmpu_ab, access: ['x'] LessThanEqual 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmpu, index: cmpu_ab, access: ['x'] MoreThan 1, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: cmpu, index: cmpu_ab, access: ['x'] MoreThanEqual 1, direction: Forward]
"'''

*/
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x' AND b = 1;
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x' AND b < 1;
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x' AND b <= 1;
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x' AND b > 1;
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x' AND b >= 1;
EXPLAIN SELECT VALUE id FROM cmp WHERE a = 'x';
EXPLAIN SELECT VALUE id FROM cmpu WHERE a = 'x' AND b = 1;
EXPLAIN SELECT VALUE id FROM cmpu WHERE a = 'x' AND b < 1;
EXPLAIN SELECT VALUE id FROM cmpu WHERE a = 'x' AND b <= 1;
EXPLAIN SELECT VALUE id FROM cmpu WHERE a = 'x' AND b > 1;
EXPLAIN SELECT VALUE id FROM cmpu WHERE a = 'x' AND b >= 1;
