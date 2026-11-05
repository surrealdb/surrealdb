/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "With two compound indexes sharing a leading nested-field column, the planner picks the one covering the most predicates"
issue = 5049

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { condition: 'id.parent = (cake:2) AND unread = true', source: 't' }, context: 'Db', operator: 'IndexCountScan' }], context: 'Db', operator: 'SelectProject' }"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX idx_unread ON t FIELDS id.parent, unread;
DEFINE INDEX idx_other ON t FIELDS id.parent, other;
FOR $i IN 0..40 { CREATE t CONTENT { id: { id: <string>$i, parent: type::record('cake', $i % 4) }, unread: $i % 2 = 0, other: false }; };
SELECT count() FROM t WHERE id.parent = cake:2 AND unread = true GROUP ALL;
SELECT count() FROM t WHERE id.parent = cake:2 AND unread = true GROUP ALL EXPLAIN;
