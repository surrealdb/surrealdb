/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Known gap: an equality on an indexed field against a record id with an array or object key must use the index"
issue = 7496
wip = true

[[test.results]]
value = """NONE"""

[[test.results]]
value = """NONE"""

[[test.results]]
value = """[{ id: t:1, v: x:plain }]"""

[[test.results]]
value = """[{ id: t:2, v: x:[1, 'a'] }]"""

[[test.results]]
value = """[{ id: t:3, v: x:{ k: 1 } }]"""

[[test.results]]
value = """[t:2]"""

[[test.results]]
value = """[t:3]"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= x:plain", direction: 'Forward', index: 't_v', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= x:[1, 'a']", direction: 'Forward', index: 't_v', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= x:{ k: 1 }", direction: 'Forward', index: 't_v', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= x:[1, 'a']", direction: 'Forward', index: 't_v', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """NONE"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= x:[1, 'a']", direction: 'Forward', index: 't_v', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_v ON t FIELDS v;
CREATE t:1 SET v = x:plain;
CREATE t:2 SET v = x:[1, "a"];
CREATE t:3 SET v = x:{k: 1};
SELECT VALUE id FROM t WHERE v = x:[1, "a"];
SELECT VALUE id FROM t WHERE v = x:{k: 1};
SELECT id FROM t WHERE v = x:plain EXPLAIN;
SELECT id FROM t WHERE v = x:[1, "a"] EXPLAIN;
SELECT id FROM t WHERE v = x:{k: 1} EXPLAIN;
SELECT id FROM t WHERE v IN [x:[1, "a"]] EXPLAIN;
LET $p = x:[1, "a"];
SELECT id FROM t WHERE v = $p EXPLAIN;
