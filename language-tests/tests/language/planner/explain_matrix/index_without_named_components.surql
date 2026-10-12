/**
[env]
namespace = true
database = true

[test]
reason = """
The plans of tables whose `id` names no components. A b-tree index over a stored
datetime field, a compound index over a string and a datetime, a COUNT index, an
index over `id[1]` of a typed array id and an index over a stored field of that
table, read by ranges, equalities, ORDER BY, LIMIT, counts, DELETE and UPDATE,
and aggregated views grouped by a number, by a string under `ORDER BY id`, and by
nothing. None of these reads a record-id component by name, so their access paths,
bounds, orders and counts are the plans the index analyzer gives a table with no
key structure: a datetime bound is the literal as written, a compound index
delivers its order and limit, and a view grouped by a number keeps its scan. The
index over `id[1]` lists each record once, because the declared kind of that
element of the typed array id holds no array, so its scan needs no
`DistinctRecords`; an index over an undeclared field that has recorded storing
no array lists each record once too, and its count is read from its keys.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: plain, index: by_at, access: >=d'2024-01-01T00:00:00.200Z' <d'2024-01-01T00:00:01Z', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: plain, index: by_feed_at, access: ['f'] MoreThan d'2024-01-01T00:00:00.200Z', direction: Forward, limit: 1]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        Sort [ctx: Db] [sort_keys: id ASC, keep: 1]
            IndexScan [ctx: Db] [table: plain, index: by_feed_at, access: ['f'], direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Filter [ctx: Db] [predicate: feed INSIDE ['f', 'g']]
            IndexScan [ctx: Db] [table: plain, index: by_at, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: plain, condition: feed = 'f']
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: plain, condition: at > d'2024-01-01T00:00:00Z']
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: arr, index: by_elem, access: >d'2024-01-01T00:00:00.200Z', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: id[0] = 'f']
        TableScan [ctx: Db] [table: arr, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: arr, index: by_v, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: arr, condition: v > 0]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: plain, index: by_at, access: <d'2024-01-01T00:00:00.600Z', direction: Forward]
"'''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: arr, index: by_v, access: = 2, direction: Forward]
''''

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
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: per_n, direction: Forward, predicate: n = 1, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: everything, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: per_s, direction: Backward]
''''
*/
DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD at ON plain TYPE datetime;
DEFINE FIELD feed ON plain TYPE string;
DEFINE INDEX by_at ON plain FIELDS at;
DEFINE INDEX by_feed_at ON plain FIELDS feed, at;
DEFINE INDEX feed_count ON plain COUNT WHERE feed = 'f';
DEFINE TABLE arr SCHEMALESS;
DEFINE FIELD id ON arr TYPE [string, datetime];
DEFINE INDEX by_elem ON arr FIELDS id[1];
DEFINE INDEX by_v ON arr FIELDS v;
CREATE plain:1 SET at = d'2024-01-01T00:00:00.500Z', feed = 'f' RETURN NONE;
CREATE plain:2 SET at = d'2024-01-01T00:00:01Z', feed = 'f' RETURN NONE;
CREATE plain:3 SET at = d'2024-01-01T00:00:00Z', feed = 'g' RETURN NONE;
CREATE arr:['f', d'2024-01-01T00:00:00.500Z'] SET v = 1 RETURN NONE;
CREATE arr:['f', d'2024-01-01T00:00:01Z'] SET v = 2 RETURN NONE;
EXPLAIN SELECT id FROM plain WHERE at >= d'2024-01-01T00:00:00.200Z' AND at < d'2024-01-01T00:00:01Z';
EXPLAIN SELECT id FROM plain WHERE feed = 'f' AND at > d'2024-01-01T00:00:00.200Z' ORDER BY at LIMIT 1;
EXPLAIN SELECT id FROM plain WHERE feed = 'f' ORDER BY id LIMIT 1;
EXPLAIN SELECT id FROM plain WHERE feed IN ['f', 'g'] ORDER BY at LIMIT 2;
EXPLAIN SELECT count() FROM plain WHERE feed = 'f' GROUP ALL;
EXPLAIN SELECT count() FROM plain WHERE at > d'2024-01-01T00:00:00Z' GROUP ALL;
EXPLAIN SELECT id FROM arr WHERE id[1] > d'2024-01-01T00:00:00.200Z';
EXPLAIN SELECT id FROM arr WHERE id[0] = 'f';
EXPLAIN SELECT id FROM arr WHERE v = 1 ORDER BY id;
EXPLAIN SELECT count() FROM arr WHERE v > 0 GROUP ALL;
EXPLAIN DELETE plain WHERE at < d'2024-01-01T00:00:00.600Z';
EXPLAIN UPDATE arr SET w = 1 WHERE v = 2;
DEFINE TABLE src;
DEFINE TABLE per_n AS SELECT n, count() AS c FROM src GROUP BY n;
DEFINE TABLE per_s AS SELECT s, count() AS c FROM src GROUP BY s;
DEFINE TABLE everything AS SELECT count() AS c FROM src GROUP ALL;
CREATE src:1 SET n = 1, s = 'a' RETURN NONE;
EXPLAIN SELECT * FROM per_n WHERE n = 1;
EXPLAIN SELECT * FROM everything;
EXPLAIN SELECT * FROM per_s ORDER BY id DESC;
