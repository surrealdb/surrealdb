/**
[test]
reason = "ORDER BY on nested aliases (AS b.c) must sort correctly across both planners"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ a: item:5, b: { c: d'2025-01-05T00:00:00Z', d: d'2025-01-05T00:00:00Z' } }, { a: item:4, b: { c: d'2025-01-04T00:00:00Z', d: d'2025-01-04T00:00:00Z' } }, { a: item:3, b: { c: d'2025-01-03T00:00:00Z', d: d'2025-01-03T00:00:00Z' } }]"

*/

-- Setup: table with nested fields and indexes
{
	DEFINE TABLE item SCHEMAFULL;
	DEFINE FIELD modified ON item TYPE datetime;
	DEFINE FIELD tag ON item TYPE string;
	DEFINE FIELD nested ON item TYPE object;
	DEFINE FIELD nested.deep ON item TYPE object;
	DEFINE FIELD nested.deep.modified ON item TYPE datetime;
	DEFINE INDEX idx_modified ON item FIELDS modified;
	DEFINE INDEX idx_nested_modified ON item FIELDS nested.deep.modified;
	CREATE item:1 SET modified = d'2025-01-01T00:00:00Z', tag = 'a', nested = { deep: { modified: d'2025-01-01T00:00:00Z' } };
	CREATE item:2 SET modified = d'2025-01-02T00:00:00Z', tag = 'a', nested = { deep: { modified: d'2025-01-02T00:00:00Z' } };
	CREATE item:3 SET modified = d'2025-01-03T00:00:00Z', tag = 'a', nested = { deep: { modified: d'2025-01-03T00:00:00Z' } };
	CREATE item:4 SET modified = d'2025-01-04T00:00:00Z', tag = 'b', nested = { deep: { modified: d'2025-01-04T00:00:00Z' } };
	CREATE item:5 SET modified = d'2025-01-05T00:00:00Z', tag = 'b', nested = { deep: { modified: d'2025-01-05T00:00:00Z' } };
	RETURN "OK";
};

-- Non-grouped: ORDER BY nested alias DESC with LIMIT
SELECT id AS a, modified AS b.c, nested.deep.modified AS b.d FROM item ORDER BY b.c DESC LIMIT 3;
