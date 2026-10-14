/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """A LET of `$parent` is refused, as `$parent` is a name the system binds for a \
subquery, so no session binding of it reaches a correlated subquery. A subquery \
reads the outer row as `$parent` whether its projection or its WHERE reads it, \
and the statements that follow a refused LET answer as they do without one."""

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
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
value = "[{ name: 'alpha', sub: { pname: 'alpha' } }, { name: 'beta', sub: { pname: 'beta' } }]"

[[test.results]]
value = "[{ name: 'alpha', sub: [{ name: 'alpha' }] }, { name: 'beta', sub: [{ name: 'beta' }] }]"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

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

-- A LET of $parent is refused, so the results that follow match the baseline.
LET $parent = "should_not_appear";
SELECT name, (SELECT $parent.name AS pname FROM ONLY true) AS sub FROM items ORDER BY name;

-- $parent in a subquery WHERE is the outer document, so the subquery finds
-- the matching item.
SELECT name, (SELECT name FROM items WHERE name = $parent.name) AS sub FROM items ORDER BY name;

-- The same query after a refused LET of $parent reads the outer row's name.
LET $parent = "should_not_appear";
SELECT name, (SELECT name FROM items WHERE name = $parent.name) AS sub FROM items ORDER BY name;
