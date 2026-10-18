/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A closed range on the second column of a compound index, behind an equality on the first, must push both bounds into the index access"
issue = 7432
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[9, 8, 7, 6, 5]"

[[test.results]]
value = """{ attributes: { expr: 'd' }, children: [{ attributes: { access: "['Paid'] MoreThanEqual 5 LessThan 10", direction: 'Backward', index: 'idx', limit: '50', table: 'inv' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"""
*/

DEFINE TABLE inv SCHEMALESS;
DEFINE INDEX idx ON inv FIELDS status, d;
FOR $i IN 0..30 { CREATE type::record('inv', $i) SET status = 'Paid', d = $i; };
SELECT VALUE d FROM inv WHERE d >= 5 AND d < 10 AND status = 'Paid' ORDER BY d DESC LIMIT 50;
SELECT VALUE d FROM inv WHERE d >= 5 AND d < 10 AND status = 'Paid' ORDER BY d DESC LIMIT 50 EXPLAIN;
