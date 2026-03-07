/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "OPTION PLANNER allows overriding the planner strategy per query batch"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:foo, name: 'foo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table Keys' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:foo, name: 'bar' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: count]
    Compute [ctx: Db] [fields: count = count(...)]
        TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = "{ attributes: { projections: 'count' }, children: [{ attributes: { fields: 'count = count(...)' }, children: [{ attributes: { direction: 'Forward', table: 'person' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'count', sql: 'count(...)' }], operator: 'Compute' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: person:foo, name: 'baz' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: count]
    Compute [ctx: Db] [fields: count = count(...)]
        TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = "{ attributes: { projections: 'count' }, children: [{ attributes: { fields: 'count = count(...)' }, children: [{ attributes: { direction: 'Forward', table: 'person' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'count', sql: 'count(...)' }], operator: 'Compute' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: person:foo, name: 'gary' }]"

*/

DEFINE TABLE person;

CREATE person:foo SET name = 'foo';

OPTION PLANNER = "compute-only";

-- Should fail
EXPLAIN SELECT count() AS count FROM person;
-- Should succeed
SELECT count() AS count FROM person EXPLAIN;
UPDATE person:foo SET name = 'bar';

OPTION PLANNER = "best-effort";
-- All Should succeed
EXPLAIN SELECT count() AS count FROM person;
SELECT count() AS count FROM person EXPLAIN;
UPDATE person:foo SET name = 'baz';

OPTION PLANNER = "all-read-only";

-- Should succeed
EXPLAIN SELECT count() AS count FROM person;
SELECT count() AS count FROM person EXPLAIN;
UPDATE person:foo SET name = 'gary';
