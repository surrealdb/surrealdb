/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "count(->edge) > 0 in WHERE should inject LIMIT 1 into graph traversal for short-circuit"

[[test.results]]
value = "[{ id: node:a, name: 'A' }]"

[[test.results]]
value = "[{ id: node:b, name: 'B' }]"

[[test.results]]
value = "[{ id: node:c, name: 'C' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = "[{ name: 'B' }, { name: 'C' }]"
*/

-- Setup: create nodes and edges
CREATE node:a SET name = "A";
CREATE node:b SET name = "B";
CREATE node:c SET name = "C";
LET $_ = (RELATE node:a->connects->node:b);
LET $_ = (RELATE node:a->connects->node:c);

-- Test: count(->connects) > 0 should find node:a (has edges)
SELECT name FROM node WHERE count(->connects) > 0;

-- Test: count(->connects) >= 1 should also find node:a
SELECT name FROM node WHERE count(->connects) >= 1;

-- Test: count(->connects) = 0 should find nodes without edges (not rewritten)
SELECT name FROM node WHERE count(->connects) = 0;
