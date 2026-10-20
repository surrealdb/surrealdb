/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An equality on an indexed field against a record id with an array or object key must use the index; a key holding a datetime answers a range from the rows, since its key text does not order chronologically, and one holding a regex is never an index operand"
issue = 7496

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

[[test.results]]
value = """[{ id: t:4, v: x:[d'2024-01-01T00:00:00Z'] }]"""

[[test.results]]
value = """[{ id: t:5, v: x:[d'2024-01-01T00:00:00.500Z'] }]"""

[[test.results]]
value = """[t:3, t:5]"""

[[test.results]]
value = """[{ count: 1 }]"""

[[test.results]]
value = """[t:5]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """NONE"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[t:2]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[]"""

[[test.results]]
value = """[]"""

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
CREATE t:4 SET v = x:[d'2024-01-01T00:00:00Z'];
CREATE t:5 SET v = x:[d'2024-01-01T00:00:00.5Z'];
SELECT VALUE id FROM t WHERE v > x:[d'2024-01-01T00:00:00Z'] ORDER BY id;
SELECT count() FROM t WHERE v < x:[d'2024-01-01T00:00:00Z'] AND v > x:[0] GROUP ALL;
SELECT VALUE id FROM t WHERE v = x:[d'2024-01-01T00:00:00.5Z'];
SELECT VALUE id FROM t WHERE v = x:[/abc/];
DEFINE INDEX t_tags ON t FIELDS tags;
SELECT VALUE id FROM t WHERE tags CONTAINS x:[/abc/];
SELECT VALUE id FROM t WHERE x:[/abc/] INSIDE tags;
SELECT VALUE id FROM t WHERE v IN [x:[1, "a"], x:[/abc/]] ORDER BY id;
SELECT VALUE id FROM t WHERE tags CONTAINSANY [x:[/abc/]];
SELECT VALUE id FROM t WHERE v = { r: x:[/abc/] };
SELECT VALUE id FROM t WHERE v < { r: x:[d'2024-01-01T00:00:00.5Z'] } AND v > { r: x:[d'2024-01-01T00:00:00Z'] };
SELECT VALUE id FROM t WHERE v = x:[/abc/..'z'];
SELECT VALUE id FROM t WHERE v > x:[d'2024-01-01T00:00:00Z'..d'2024-01-01T00:00:01Z'] AND v < x:[d'2024-01-02T00:00:00Z'..];
