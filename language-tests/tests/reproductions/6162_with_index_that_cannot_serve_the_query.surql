/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "WITH INDEX naming an index the query cannot use is reported instead of silently scanning"
issue = 6162

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, isActive: true, order: 2 }]"

[[test.results]]
value = "[{ id: test:2, isActive: true, order: 1 }]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { sort_keys: 'order ASC' }, children: [{ attributes: { predicate: 'isActive' }, children: [{ attributes: { direction: 'Forward', table: 'test' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: 'other = 1', table: 'test' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE test;
DEFINE INDEX idx_isActive_order ON test FIELDS isActive, order;
CREATE test:1 SET isActive = true, order = 2;
CREATE test:2 SET isActive = true, order = 1;
SELECT VALUE id FROM test WITH INDEX idx_isActive_order WHERE isActive ORDER BY order EXPLAIN;
SELECT VALUE id FROM test WITH INDEX idx_isActive_order WHERE other = 1 EXPLAIN;
