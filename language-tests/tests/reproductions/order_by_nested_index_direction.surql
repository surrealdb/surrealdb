/**
[test]
reason = "ORDER BY on indexed nested field must use backward index scan with LIMIT pushdown instead of full scan + in-memory sort"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:5, modified: d'2025-01-05T00:00:00Z' }, { id: item:4, modified: d'2025-01-04T00:00:00Z' }, { id: item:3, modified: d'2025-01-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: item:1, modified: d'2025-01-01T00:00:00Z' }, { id: item:2, modified: d'2025-01-02T00:00:00Z' }, { id: item:3, modified: d'2025-01-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: item:5, nested: { deep: { modified: d'2025-01-05T00:00:00Z' } } }, { id: item:4, nested: { deep: { modified: d'2025-01-04T00:00:00Z' } } }, { id: item:3, nested: { deep: { modified: d'2025-01-03T00:00:00Z' } } }]"

[[test.results]]
value = "[{ id: item:1, nested: { deep: { modified: d'2025-01-01T00:00:00Z' } } }, { id: item:2, nested: { deep: { modified: d'2025-01-02T00:00:00Z' } } }, { id: item:3, nested: { deep: { modified: d'2025-01-03T00:00:00Z' } } }]"
*/

-- Setup: table with nested fields and indexes
{
	DEFINE TABLE item SCHEMAFULL;
	DEFINE FIELD modified ON item TYPE datetime;
	DEFINE FIELD nested ON item TYPE object;
	DEFINE FIELD nested.deep ON item TYPE object;
	DEFINE FIELD nested.deep.modified ON item TYPE datetime;
	DEFINE INDEX idx_modified ON item FIELDS modified;
	DEFINE INDEX idx_nested_modified ON item FIELDS nested.deep.modified;
	CREATE item:1 SET modified = d'2025-01-01T00:00:00Z', nested = { deep: { modified: d'2025-01-01T00:00:00Z' } };
	CREATE item:2 SET modified = d'2025-01-02T00:00:00Z', nested = { deep: { modified: d'2025-01-02T00:00:00Z' } };
	CREATE item:3 SET modified = d'2025-01-03T00:00:00Z', nested = { deep: { modified: d'2025-01-03T00:00:00Z' } };
	CREATE item:4 SET modified = d'2025-01-04T00:00:00Z', nested = { deep: { modified: d'2025-01-04T00:00:00Z' } };
	CREATE item:5 SET modified = d'2025-01-05T00:00:00Z', nested = { deep: { modified: d'2025-01-05T00:00:00Z' } };
	RETURN "OK";
};

-- Test 1: ORDER BY top-level indexed field DESC with LIMIT
-- Should use backward index scan, NOT in-memory sort
SELECT id, modified FROM item ORDER BY modified DESC LIMIT 3;

-- Test 2: ORDER BY top-level indexed field ASC with LIMIT
-- Should use forward index scan
SELECT id, modified FROM item ORDER BY modified ASC LIMIT 3;

-- Test 3: ORDER BY nested indexed field DESC with LIMIT
-- This is the core regression: should use backward index scan on nested field
SELECT id, nested.deep.modified FROM item ORDER BY nested.deep.modified DESC LIMIT 3;

-- Test 4: ORDER BY nested indexed field ASC with LIMIT
SELECT id, nested.deep.modified FROM item ORDER BY nested.deep.modified ASC LIMIT 3;
