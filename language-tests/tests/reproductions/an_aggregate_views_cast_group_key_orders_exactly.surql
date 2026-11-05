/**
[env]
namespace = true
database = true
timeout = 15000

[test]
reason = """
	An aggregated view stores each group under a record id built from its
	group values, with no coercion on the way in, so a group column holds a
	kind only where its group expression fixes one: a cast yields a value of
	its target kind or raises. A view grouped by a cast to a kind whose key
	bytes order exactly therefore does sort by `id` in storage order, and
	`ORDER BY id` over it needs no `Sort`, while a view grouped by a bare
	field read — whose values are whatever the source rows hold — keeps one,
	as does a key with one such column beside a cast. A key range over a key
	whose columns all order exactly reads in key order as well, so a bounded
	read under `ORDER BY id` keeps no `Sort` either.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[by_text:['a'], by_text:['b']]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: by_text, direction: Forward]
''''

[[test.results]]
value = "[by_num:[2], by_num:[1]]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: by_num, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        TableScan [ctx: Db] [table: by_field, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        TableScan [ctx: Db] [table: by_both, direction: Forward]
''''

[[test.results]]
value = "[by_text:['a'], by_text:['b']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    KeyRangeScan [ctx: Db] [table: by_text, range: id[0] >= 'a', direction: Forward, residual: yes, predicate: k >= 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[by_num_range:[2]]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KeyRangeScan [ctx: Db] [table: by_num_range, range: id[0] >= 2, direction: Forward, residual: yes, predicate: k >= 2, pre_decode_filter: yes]
''''
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE by_text AS SELECT <string> s AS k, count() AS n FROM src GROUP BY k;
DEFINE TABLE by_num AS SELECT <int> v AS k, count() AS n FROM src GROUP BY k;
DEFINE TABLE by_field AS SELECT s, count() AS n FROM src GROUP BY s;
DEFINE TABLE by_both AS SELECT <string> s AS k, v, count() AS n FROM src GROUP BY k, v;
-- Written out of key order, so a scan that kept no sort would be caught.
CREATE src SET s = 'b', v = 2 RETURN NONE;
CREATE src SET s = 'a', v = 1 RETURN NONE;

SELECT VALUE id FROM by_text ORDER BY id;
EXPLAIN SELECT VALUE id FROM by_text ORDER BY id;
SELECT VALUE id FROM by_num ORDER BY id DESC;
EXPLAIN SELECT VALUE id FROM by_num ORDER BY id DESC;
EXPLAIN SELECT VALUE id FROM by_field ORDER BY id;
EXPLAIN SELECT VALUE id FROM by_both ORDER BY id;
-- A key range over such a key reads it in key order too.
SELECT VALUE id FROM by_text WHERE k >= 'a' ORDER BY id;
EXPLAIN SELECT VALUE id FROM by_text WHERE k >= 'a' ORDER BY id;
-- An int group fixed by its cast is bounded as an int component is.
DEFINE TABLE by_num_range AS SELECT <int> v AS k, count() AS n FROM src GROUP BY k;
SELECT VALUE id FROM by_num_range WHERE k >= 2 ORDER BY id;
EXPLAIN SELECT VALUE id FROM by_num_range WHERE k >= 2 ORDER BY id;
