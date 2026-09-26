/**
[env]
planner-strategy = ["all-ro"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: person:1 }]"

[[test.results]]
value = "[{ address: { city: 'Berlin' }, id: person:2 }]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { access: "= 'Berlin'", direction: 'Forward', index: 'person_city' }, context: 'Db', metrics: { output_rows: 1 }, operator: 'IndexScan' }], context: 'Db', expressions: [{ role: 'expr', sql: 'id' }], metrics: { output_rows: 1 }, operator: 'ProjectValue', total_rows: 1 }"""

[[test.results]]
value = "[person:2]"
*/
-- Issue #7524, companion to `on_schemafull_nullable_object_sub_field.surql`:
-- proves the index defined over a sub-field of a `null | { ... }` parent is
-- actually *used* by the planner, rather than the query silently falling back
-- to a table scan and passing the value assertions anyway.
--
-- `planner-strategy` is pinned because the EXPLAIN rendering differs per
-- strategy; the gate under test is strategy-independent, and the unpinned
-- companion file covers it under every strategy.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD address ON person TYPE null | { city: string } DEFAULT NULL;
DEFINE INDEX person_city ON person FIELDS address.city;
CREATE person:1 SET address = NULL;
CREATE person:2 SET address = { city: 'Berlin' };
SELECT VALUE id FROM person WHERE address.city = 'Berlin' EXPLAIN FULL;
SELECT VALUE id FROM person WHERE address.city = 'Berlin';
