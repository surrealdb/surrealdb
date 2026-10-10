/**
[env]
imports = ["upgrade/define/3.00.00_index_unique_nullish_single_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_unique_nullish_single.surql are answered
by: equality on NULL, on NONE and on a complete value each scan the index.
Without these an index the new binary cannot read would fall back to a table
scan and return the same rows.

`EXPLAIN` output is not stable across the 3.x line, so the upgrade target is
the current checkout; the seed axis sends every released version straight to
it."""

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: solo, index: solo_v, access: = NULL, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: solo, index: solo_v, access: = NONE, direction: Forward]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: solo, index: solo_v, access: = 'a', direction: Forward]
"'''

*/
EXPLAIN SELECT VALUE id FROM solo WHERE v = NULL;
EXPLAIN SELECT VALUE id FROM solo WHERE v = NONE;
EXPLAIN SELECT VALUE id FROM solo WHERE v = 'a';
