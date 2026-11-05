/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY with mixed ASC/DESC directions over a compound index returns correctly ordered rows"
issue = 7030

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[0, 9], [0, 6], [0, 3], [0, 0], [1, 10]]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { limit: '5' }, children: [{ attributes: { keep: '5', sort_keys: 'status ASC, created_at DESC' }, children: [{ attributes: { direction: 'Forward', table: 't', topk_pushdown: 'yes' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'Limit' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ix ON t FIELDS status, created_at;
FOR $i IN 0..12 { CREATE type::record('t', $i) SET status = $i % 3, created_at = $i; };
SELECT VALUE [status, created_at] FROM t ORDER BY status ASC, created_at DESC LIMIT 5;
SELECT VALUE id FROM t ORDER BY status ASC, created_at DESC LIMIT 5 EXPLAIN;
