/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A bare truthiness filter WHERE f ORDER BY s uses the (s, f) index for ordering and filters on f, while an equality filter WHERE f = true ORDER BY s uses the (f, s) compound index for both scan and sort"
issue = 6159

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]"

[[test.results]]
value = "{ attributes: { expr: 's' }, children: [{ attributes: { sort_keys: 's ASC' }, children: [{ attributes: { predicate: 'f' }, children: [{ attributes: { direction: 'Forward', table: 'test' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 's' }, children: [{ attributes: { predicate: 'f' }, children: [{ attributes: { direction: 'Forward', index: 'idx_s_f', table: 'test' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]"

[[test.results]]
value = "{ attributes: { expr: 's' }, children: [{ attributes: { access: '[true]', direction: 'Forward', index: 'idx_f_s', table: 'test' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 's' }, children: [{ attributes: { sort_keys: 's ASC' }, children: [{ attributes: { predicate: 'f' }, children: [{ attributes: { direction: 'Forward', table: 'test' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 's' }, children: [{ attributes: { access: '[true]', direction: 'Forward', index: 'idx_f_s', table: 'test' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE test;
DEFINE INDEX idx_f_s ON test FIELDS f, s;
DEFINE INDEX idx_s_f ON test FIELDS s, f;
FOR $i IN 0..20 { CREATE type::record('test', $i) SET s = $i, f = $i % 2 = 0; };
SELECT VALUE s FROM test WITH INDEX idx_f_s WHERE f ORDER BY s;
SELECT VALUE s FROM test WITH INDEX idx_f_s WHERE f ORDER BY s EXPLAIN;
SELECT VALUE s FROM test WHERE f ORDER BY s EXPLAIN;
SELECT VALUE s FROM test WHERE f = true ORDER BY s;
SELECT VALUE s FROM test WHERE f = true ORDER BY s EXPLAIN;
REMOVE INDEX idx_s_f ON test;
SELECT VALUE s FROM test WITH INDEX idx_f_s WHERE f ORDER BY s EXPLAIN;
SELECT VALUE s FROM test WITH INDEX idx_f_s WHERE f = true ORDER BY s EXPLAIN;
