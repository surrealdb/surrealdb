/**
[env]
namespace = true
database = true
auth = { level = "owner" }

planner-strategy = ["all-ro"]

[test]
reason = "Computed fields containing foreign SELECTs should not cause severe performance regressions or timeouts"

[[test.results]]
value = "[{ id: finding:1, name: 'f1' }]"

[[test.results]]
value = "[{ id: finding:2, name: 'f2' }]"

[[test.results]]
value = "[{ id: finding:3, name: 'f3' }]"

[[test.results]]
value = "[{ finding: finding:1, id: finding_annotation:1, note: 'annotation for f1' }]"

[[test.results]]
value = "[{ finding: finding:2, id: finding_annotation:2, note: 'annotation for f2' }]"

[[test.results]]
value = "[{ id: finding:1, name: 'f1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''[{ id: finding:1, name: 'f1', test: 'SelectProject [ctx: Db] [projections: *]\n    DynamicScan [ctx: Db] [source: finding_annotation, predicate: finding = $value, limit: 1]\n' }]'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: finding:1, name: 'f1', test: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''[{ id: finding:1, name: 'f1', test: 'SelectProject [ctx: Db] [projections: *]\n    DynamicScan [ctx: Db] [source: finding_annotation, limit: 1]\n' }]'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: finding:1, name: 'f1' }]"

*/

-- Set up two tables: finding and finding_annotation
CREATE finding:1 SET name = "f1";
CREATE finding:2 SET name = "f2";
CREATE finding:3 SET name = "f3";

CREATE finding_annotation:1 SET finding = finding:1, note = "annotation for f1";
CREATE finding_annotation:2 SET finding = finding:2, note = "annotation for f2";

-- Baseline: select without computed field
SELECT * FROM finding LIMIT 1 TIMEOUT 500ms;

-- Define a computed field that does a foreign SELECT with a WHERE referencing $value
DEFINE FIELD OVERWRITE test ON TABLE finding COMPUTED {
	RETURN EXPLAIN SELECT * FROM finding_annotation WHERE finding = $value LIMIT 1
};

SELECT * FROM finding LIMIT 1 TIMEOUT 500ms;

-- Redefine computed field to select from an empty array (should be fast)
DEFINE FIELD OVERWRITE test ON TABLE finding COMPUTED {
	RETURN (SELECT * FROM [])
};

SELECT * FROM finding LIMIT 1 TIMEOUT 500ms;

-- Redefine computed field to select from the foreign table with a LIMIT (should be fast)
DEFINE FIELD OVERWRITE test ON TABLE finding COMPUTED {
	RETURN EXPLAIN SELECT * FROM finding_annotation LIMIT 1
};

SELECT * FROM finding LIMIT 1 TIMEOUT 500ms;

-- Clean up: remove the computed field
REMOVE FIELD test ON TABLE finding;

SELECT * FROM finding LIMIT 1 TIMEOUT 500ms;
