/**
[env]
namespace = true
database = true

[test]
reason = "Pins the execution behaviors the GQL v2 MatchPlan operators rely on (V2_DESIGN §3/§4): the 3VL guard trio that the lowering inserts before every comparison, record::tb table filtering on an edge endpoint, RecordId auto-fetch of a one-level-deeper link, the NULL/NONE sort position used for ORDER BY over non-returned bindings (R7), and NULL/NONE equality under SurrealQL's total order. Runs as plain SurrealQL; if any of these change, the v2 operators (Bind/Expand/PathExpand/Sort/Filter) must be revisited. Modeled on lowering_substrate.surql."

# Seed statements (5): all RETURN NONE -> empty results.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# S1: guard trio, age > 100 false for all.
[[test.results]]
value = "[]"

# S2: record::tb endpoint filter matches nothing.
[[test.results]]
value = "[]"

# S3: SELECT * FROM [] is empty.
[[test.results]]
value = "[]"

# S4: (NULL = NULL) = false is false -> no rows.
[[test.results]]
value = "[]"

# S5: guard trio admits person:1 (age 30 > 25).
[[test.results]]
value = "[{ id: person:1 }]"

# S6: NONE sorts first ascending.
[[test.results]]
value = "[{ age: NONE, id: person:3 }, { age: 20, id: person:2 }, { age: 30, id: person:1 }]"

# S7: NONE sorts last descending.
[[test.results]]
value = "[{ age: 30, id: person:1 }, { age: 20, id: person:2 }, { age: NONE, id: person:3 }]"

# S8: RecordId auto-fetch through edge.out -> person:2 name.
[[test.results]]
value = "[{ name: 'B' }]"

# S9: NULL/NONE equality + ordering pins.
[[test.results]]
value = "[true, true, false, true, false]"
*/

-- Seed: people (one without age — person:3), deterministic ids; a person->city
-- edge and a person->person edge to exercise record::tb endpoint filtering.
CREATE person:1 SET name = 'A', age = 30 RETURN NONE;
CREATE person:2 SET name = 'B', age = 20 RETURN NONE;
CREATE person:3 SET name = 'C' RETURN NONE;
CREATE city:1 SET name = 'London' RETURN NONE;
INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2, since: 2021 },
	{ id: knows:k1c, in: person:1, out: city:1, since: 2019 }
] RETURN NONE;

-- S1: the guard trio (x != NONE AND x != NULL AND x > v) — the shape the v2
-- lowering inserts into Expand/Filter predicates. person:3 has no age and must
-- be excluded; here the predicate is false for everyone (age > 100).
SELECT id FROM person WHERE age != NONE AND age != NULL AND age > 100 ORDER BY id;

-- S2: record::tb on an edge's `out` endpoint keeps only person->person edges
-- (the far-label filter the planner applies to Expand targets). Empty here
-- because we project nothing matching after the guard rejects the city edge.
SELECT id FROM knows WHERE record::tb(out) = 'nonexistent' ORDER BY id;

-- S3: an empty array binding stays empty (group-var [] shape for zero-length).
SELECT * FROM [] ORDER BY id;

-- S4: NULL != NULL is false, so a NULL key never equi-joins (R3/join rule).
SELECT id FROM person WHERE (NULL = NULL) = false ORDER BY id;

-- S5: guard trio admits exactly the rows with a present, qualifying field.
SELECT id FROM person WHERE age != NONE AND age != NULL AND age > 25 ORDER BY id;

-- S6: NONE sorts FIRST ascending — the NULL/NONE position the v2 streaming Sort
-- uses for an ORDER BY over a binding (R7). person:3 (no age) leads. NOTE the
-- field must be selected here because SurrealQL's SELECT rejects an ORDER BY on a
-- non-projected idiom; the v2 GQL path instead sorts the full binding row
-- pre-projection, which is exactly why R7 can order by a non-returned binding.
SELECT id, age FROM person ORDER BY age ASC;

-- S7: NONE sorts LAST descending — the mirror of S6.
SELECT id, age FROM person ORDER BY age DESC;

-- S8: RecordId auto-fetch through a one-level-deeper link (edge.out -> node),
-- the FieldState-aware path v2 binding fetch relies on; the person:1->person:2
-- edge resolves its target's name.
SELECT out.name AS name FROM knows WHERE record::tb(out) = 'person' ORDER BY name;

-- S9: NULL/NONE equality + ordering pins under SurrealQL's total order.
RETURN [NULL = NULL, NONE = NONE, NULL = NONE, NONE < 1, NULL > 1];
