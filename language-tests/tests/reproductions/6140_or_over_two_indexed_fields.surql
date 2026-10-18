/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An OR whose branches are each indexed is answered from the indexes"
issue = 6140

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = 1 OR b = 4' }, children: [{ attributes: { table: 't' }, children: [{ children: [{ attributes: { access: '= 1', index: 'ia' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 4', index: 'ib' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapOr' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
DEFINE INDEX ib ON t FIELDS b;
FOR $i IN 0..10 { CREATE type::record('t', $i) SET a = $i, b = $i * 2; };
SELECT VALUE id FROM t WHERE a = 1 OR b = 4 ORDER BY id;
SELECT VALUE id FROM t WHERE a = 1 OR b = 4 EXPLAIN;
