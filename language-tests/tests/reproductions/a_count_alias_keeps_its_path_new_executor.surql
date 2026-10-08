/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The plans behind `a_count_alias_keeps_its_path`, which asserts the
	rows. A count-only `GROUP ALL` is answered by `CountScan` without
	materialising a row, and by `IndexCountScan` when a `WHERE` is answered by
	an index. Neither builds a nested row, so a statement whose alias names a
	path takes the ordinary aggregation over a scan instead.

	Both directions are pinned: the shortcut still answers a plain alias, and
	stops answering a structured one. Declining everything would satisfy the
	row assertions alone.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "[{ id: t:2, k: 1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    CountScan [ctx: Db] [source: t]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: t, condition: k = 1]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    CountScan [ctx: Db] [source: t]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        IndexScan [ctx: Db] [table: t, index: t_k, access: = 1, direction: Forward]
''''
*/

-- Index keys are counted only for a column declared to hold no array.
DEFINE FIELD k ON t TYPE int;
DEFINE INDEX t_k ON t FIELDS k;
CREATE t:1 SET k = 1;
CREATE t:2 SET k = 1;

-- A plain alias is one key, and the shortcuts answer it.
EXPLAIN SELECT count() AS n FROM t GROUP ALL;
EXPLAIN SELECT count() AS n FROM t WHERE k = 1 GROUP ALL;

-- A quoted key with a dot in it is still one key.
EXPLAIN SELECT count() AS `foo.bar` FROM t GROUP ALL;

-- A path is not, so the aggregation answers it.
EXPLAIN SELECT count() AS foo.bar FROM t GROUP ALL;
EXPLAIN SELECT count() AS foo.bar FROM t WHERE k = 1 GROUP ALL;
