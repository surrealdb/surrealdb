/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "`tags CONTAINS v` over an index on `tags.*` that has recorded storing nothing whole seeks `v`, `NONE` and `NULL`: a record whose `tags` is a scalar (a string holding `v` as a substring) is listed under `NONE`, so the seek finds it and the predicate, judged again over every record the seek yields, keeps it. Once a record stores an object in the column (`tags.*` keeps it whole) the seek is no longer complete and the table is scanned. The ordered seek serves `ORDER BY` the index's next column with a limit; with neither an `ORDER BY` nor a `LIMIT` the seeks are read too, and under a `LIMIT` they cannot order for the table is scanned, which stops as early."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2020-01-01T00:00:00Z', id: item:a, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ at: d'2020-01-02T00:00:00Z', id: item:b, tags: ['y'] }]"

[[test.results]]
value = "[{ at: d'2020-01-03T00:00:00Z', id: item:c, tags: 'axb' }]"

[[test.results]]
value = "[{ at: d'2020-01-04T00:00:00Z', id: item:d }]"

[[test.results]]
value = "[{ at: d'2020-01-05T00:00:00Z', id: item:e, tags: [] }]"

[[test.results]]
value = "[{ at: d'2020-01-06T00:00:00Z', id: item:f, tags: ['z', 'x'] }]"

[[test.results]]
value = "[{ at: d'2020-01-07T00:00:00Z', id: item:g, tags: NULL }]"

[[test.results]]
value = "[item:f, item:c, item:a]"

[[test.results]]
value = "[item:f, item:c]"

[[test.results]]
value = "[item:a, item:c, item:f]"

[[test.results]]
value = "[item:a, item:b, item:f]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Limit [ctx: Db] [limit: 2]
        Filter [ctx: Db] [predicate: tags CONTAINS 'x']
            UnionIndexScan [ctx: Db] [table: item, branches: 3, merge_by_index_key_dedup: at Desc]
                IndexScan [ctx: Db] [table: item, index: ix_tags, access: ['x'], direction: Backward]
                IndexScan [ctx: Db] [table: item, index: ix_tags, access: [NONE], direction: Backward]
                IndexScan [ctx: Db] [table: item, index: ix_tags, access: [NULL], direction: Backward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'x']
        UnionIndexScan [ctx: Db] [table: item, branches: 3]
            IndexScan [ctx: Db] [table: item, index: ix_tags, access: ['x'], direction: Forward]
            IndexScan [ctx: Db] [table: item, index: ix_tags, access: [NONE], direction: Forward]
            IndexScan [ctx: Db] [table: item, index: ix_tags, access: [NULL], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: item, direction: Forward, predicate: tags CONTAINS 'x', limit: 2, pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ at: d'2020-01-08T00:00:00Z', id: item:h, tags: { x: 1 } }]"

[[test.results]]
value = "[item:h, item:f, item:c, item:a]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: at DESC, keep: 2]
            TableScan [ctx: Db] [table: item, direction: Forward, predicate: tags CONTAINS 'x', pre_decode_filter: yes, topk_pushdown: yes]
"'''

*/

DEFINE INDEX ix_tags ON item FIELDS tags.*, at;
CREATE item:a SET tags = ['x', 'y'], at = d'2020-01-01T00:00:00Z';
CREATE item:b SET tags = ['y'], at = d'2020-01-02T00:00:00Z';
CREATE item:c SET tags = 'axb', at = d'2020-01-03T00:00:00Z';
CREATE item:d SET at = d'2020-01-04T00:00:00Z';
CREATE item:e SET tags = [], at = d'2020-01-05T00:00:00Z';
CREATE item:f SET tags = ['z', 'x'], at = d'2020-01-06T00:00:00Z';
CREATE item:g SET tags = NULL, at = d'2020-01-07T00:00:00Z';

SELECT VALUE id FROM item WHERE tags CONTAINS 'x' ORDER BY at DESC;
SELECT VALUE id FROM item WHERE tags CONTAINS 'x' ORDER BY at DESC LIMIT 2;
SELECT VALUE id FROM item WHERE 'x' INSIDE tags ORDER BY at;
SELECT VALUE id FROM item WHERE tags CONTAINSANY ['y', 'z'] ORDER BY at;
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINS 'x' ORDER BY at DESC LIMIT 2;
-- With neither an ORDER BY nor a LIMIT the seeks are read; under a LIMIT that
-- the union cannot order for, the table scan stops as early and reads in order.
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINS 'x';
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINS 'x' LIMIT 2;
SELECT count() FROM item WHERE tags CONTAINS 'x' GROUP ALL;

-- An object's key is contained too, and `tags.*` keeps an object whole.
CREATE item:h SET tags = { x: 1 }, at = d'2020-01-08T00:00:00Z';
SELECT VALUE id FROM item WHERE tags CONTAINS 'x' ORDER BY at DESC;
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINS 'x' ORDER BY at DESC LIMIT 2;
