/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1587
reason = """
The value shapes an index records are per column, per committed write and per
definition. On a compound index whose leading column holds scalars and whose
trailing column holds an array, the scan still delivers the leading column's
order, lists a record its trailing array repeats once, and answers a range on
the trailing column after an equality on the leading one with the whole-array
comparison. An array written by a transaction that is cancelled records
nothing. REMOVE INDEX takes the recorded shapes with it, so an index defined
again under the same name over records holding no array plans as a scalar
column. An equality on a compound index's leading column, ordered by its
trailing column, reads the pinned prefix backward and stops at the limit.
Every indexed read answers what its WITH NOINDEX twin answers."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: [9, 2], id: c:1 }]"

[[test.results]]
value = "[{ a: 2, b: 5, id: c:2 }]"

[[test.results]]
value = "[{ a: 3, b: 7, id: c:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: c, index: c_ab, direction: Backward]
''''

[[test.results]]
value = "[c:3, c:2]"

[[test.results]]
value = "[c:3, c:2]"

[[test.results]]
value = "[c:1, c:2, c:3]"

[[test.results]]
value = "[c:1, c:2, c:3]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Sort [ctx: Db] [sort_keys: a ASC, b ASC]
        TableScan [ctx: Db] [table: c, direction: Forward]
''''

[[test.results]]
value = "[c:1, c:2, c:3]"

[[test.results]]
value = "[c:1, c:2, c:3]"

[[test.results]]
value = "[c:1]"

[[test.results]]
value = "[c:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ created_at: d'2024-01-01T00:00:00Z', id: e:1, status: 'published' }]"

[[test.results]]
value = "[{ created_at: d'2024-01-04T00:00:00Z', id: e:2, status: 'draft' }]"

[[test.results]]
value = "[{ created_at: d'2024-01-03T00:00:00Z', id: e:3, status: 'published' }]"

[[test.results]]
value = "[{ created_at: d'2024-01-02T00:00:00Z', id: e:4, status: 'published' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: e, index: e_status_created, access: ['published'], direction: Backward, limit: 2]
"'''

[[test.results]]
value = "[e:3, e:4]"

[[test.results]]
value = "[e:3, e:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, t: 1 }]"

[[test.results]]
value = "[{ id: r:2, t: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: r, index: r_t, direction: Backward, limit: 1]
''''

[[test.results]]
value = "[r:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, t: 1 }]"

[[test.results]]
value = "[{ id: d:2, t: [2] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        Sort [ctx: Db] [sort_keys: t DESC, keep: 1]
            TableScan [ctx: Db] [table: d, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: d, index: d_t, direction: Backward, limit: 1]
''''

[[test.results]]
value = "[d:1]"

*/

DEFINE TABLE c SCHEMALESS;
DEFINE INDEX c_ab ON c FIELDS a, b;
CREATE c:1 SET a = 1, b = [9, 2];
CREATE c:2 SET a = 2, b = 5;
CREATE c:3 SET a = 3, b = 7;

-- The leading column never held an array: its order comes from the index, and
-- the record the trailing array repeats is listed once.
EXPLAIN SELECT id FROM c ORDER BY a DESC LIMIT 2;
SELECT VALUE id FROM c ORDER BY a DESC LIMIT 2;
SELECT VALUE id FROM c WITH NOINDEX ORDER BY a DESC LIMIT 2;
SELECT VALUE id FROM c ORDER BY a;
SELECT VALUE id FROM c WITH NOINDEX ORDER BY a;
-- The trailing column held an array, so it orders nothing past the leading one.
EXPLAIN SELECT id FROM c ORDER BY a, b;
SELECT VALUE id FROM c ORDER BY a, b;
SELECT VALUE id FROM c WITH NOINDEX ORDER BY a, b;
-- A range on the trailing column reaching the array band is not read from the
-- index: `[9, 2] > 3` holds by the whole array.
SELECT VALUE id FROM c WHERE a = 1 AND b > 3;
SELECT VALUE id FROM c WITH NOINDEX WHERE a = 1 AND b > 3;

-- An equality on the leading column of a compound index, ordered by the
-- trailing one: neither column has held an array, so the backward scan of the
-- pinned prefix delivers the order and stops at the limit.
DEFINE TABLE e SCHEMALESS;
DEFINE INDEX e_status_created ON e FIELDS status, created_at;
CREATE e:1 SET status = 'published', created_at = d'2024-01-01';
CREATE e:2 SET status = 'draft', created_at = d'2024-01-04';
CREATE e:3 SET status = 'published', created_at = d'2024-01-03';
CREATE e:4 SET status = 'published', created_at = d'2024-01-02';
EXPLAIN SELECT id FROM e WHERE status = 'published' ORDER BY created_at DESC LIMIT 2;
SELECT VALUE id FROM e WHERE status = 'published' ORDER BY created_at DESC LIMIT 2;
SELECT VALUE id FROM e WITH NOINDEX WHERE status = 'published' ORDER BY created_at DESC LIMIT 2;

-- A cancelled transaction records no shape.
DEFINE TABLE r SCHEMALESS;
DEFINE INDEX r_t ON r FIELDS t;
CREATE r:1 SET t = 1;
CREATE r:2 SET t = 2;
BEGIN;
CREATE r:3 SET t = [3];
CANCEL;
EXPLAIN SELECT id FROM r ORDER BY t DESC LIMIT 1;
SELECT VALUE id FROM r ORDER BY t DESC LIMIT 1;

-- A definition removed takes its shapes with it.
DEFINE TABLE d SCHEMALESS;
DEFINE INDEX d_t ON d FIELDS t;
CREATE d:1 SET t = 1;
CREATE d:2 SET t = [2];
EXPLAIN SELECT id FROM d ORDER BY t DESC LIMIT 1;
DELETE d:2;
REMOVE INDEX d_t ON d;
DEFINE INDEX d_t ON d FIELDS t;
EXPLAIN SELECT id FROM d ORDER BY t DESC LIMIT 1;
SELECT VALUE id FROM d ORDER BY t DESC LIMIT 1;
