/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """Verify that LET bindings for doc-scoped param names ($parent) do not \
interfere with correlated subquery behavior at plan time. Row-scoped params \
must be deferred to runtime so that ScalarSubquery can bind $parent per-row."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: items:1, name: 'alpha' }]"

[[test.results]]
value = "[{ id: items:2, name: 'beta' }]"

[[test.results]]
value = "[{ name: 'alpha', sub: { pname: 'alpha' } }, { name: 'beta', sub: { pname: 'beta' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'alpha', sub: { pname: 'alpha' } }, { name: 'beta', sub: { pname: 'beta' } }]"

[[test.results]]
value = "[{ name: 'alpha', sub: [{ name: 'alpha' }] }, { name: 'beta', sub: [{ name: 'beta' }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'alpha', sub: [{ name: 'alpha' }] }, { name: 'beta', sub: [{ name: 'beta' }] }]"
*/

DEFINE TABLE items SCHEMAFULL;
DEFINE FIELD name ON items TYPE string;
CREATE items:1 SET name = "alpha";
CREATE items:2 SET name = "beta";

-- Baseline: $parent.name in subquery projection (runtime path, no plan-time folding).
-- $parent should be the current outer document for each row.
SELECT name, (SELECT $parent.name AS pname FROM ONLY true) AS sub FROM items ORDER BY name;

-- LET shadow on $parent: the per-row injection at execution time should
-- still shadow the LET binding, so results should match the baseline.
LET $parent = "should_not_appear";
SELECT name, (SELECT $parent.name AS pname FROM ONLY true) AS sub FROM items ORDER BY name;

-- Critical path: $parent in subquery WHERE (affected by resolve_condition_params).
-- Without a LET shadow, $parent.name should be the outer document's name,
-- so the subquery finds the matching item.
SELECT name, (SELECT name FROM items WHERE name = $parent.name) AS sub FROM items ORDER BY name;

-- Same query WITH a LET shadow. If plan-time resolution incorrectly folds
-- $parent to "should_not_appear", then $parent.name would be NONE and
-- the subquery would return no results.
LET $parent = "should_not_appear";
SELECT name, (SELECT name FROM items WHERE name = $parent.name) AS sub FROM items ORDER BY name;
