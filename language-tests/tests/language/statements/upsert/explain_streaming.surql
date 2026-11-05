/**
[env]

[test]
reason = "The EXPLAIN shape for a mutation: one write operator carrying the per-record stage list of each order it runs, over the source its subjects come from."

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = """{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'bar'", direction: 'Forward', index: 'idx', table: 'person' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

*/

CREATE person:test SET name = 'foo';
DEFINE INDEX idx ON person FIELDS name;
UPSERT person:test SET name = 'bar' EXPLAIN ;
SELECT * FROM person;
UPSERT person SET name = 'bar' EXPLAIN;
SELECT * FROM person;
UPSERT person SET name = 'bar' WHERE name = 'bar' EXPLAIN;
SELECT * FROM person;
UPSERT person SET name = 'bar' WHERE name = 'foo';
SELECT * FROM person;