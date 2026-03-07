/**
[test]
reason = "Test SELECT ... WITH NOINDEX and WITH INDEX to exercise the With enum"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, name: 'alice' }]"

[[test.results]]
value = "[{ id: item:2, name: 'bob' }]"

[[test.results]]
value = "[{ id: item:1, name: 'alice' }]"

[[test.results]]
value = "[{ id: item:1, name: 'alice' }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', predicate: "name = 'alice'", table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"""

*/

DEFINE TABLE item;
DEFINE INDEX idx_name ON item FIELDS name;
CREATE item:1 SET name = 'alice';
CREATE item:2 SET name = 'bob';

SELECT * FROM item WITH NOINDEX WHERE name = 'alice';
SELECT * FROM item WITH INDEX idx_name WHERE name = 'alice';
SELECT * FROM item WITH NOINDEX WHERE name = 'alice' EXPLAIN;
