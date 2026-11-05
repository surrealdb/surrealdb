/**
[env]

[test]
reason = "The EXPLAIN shape for a mutation: one write operator carrying the per-record stage list of each order it runs, over the source its subjects come from."

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { record_id: 'person:test' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'person' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = """{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'Tester'", direction: 'Forward', index: 'idx', table: 'person' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE person:test SET name = 'Tester';
DEFINE INDEX idx ON person FIELDS name;
DELETE person:test EXPLAIN;
SELECT * FROM person;
DELETE person EXPLAIN;
SELECT * FROM person;
DELETE person WHERE name = 'Tester' EXPLAIN;
SELECT * FROM person;
DELETE person WHERE name = 'Tester';
SELECT * FROM person;