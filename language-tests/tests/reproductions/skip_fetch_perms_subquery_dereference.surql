/**
[test]
reason = "Permission predicates containing subqueries that dereference linked records must propagate the skip_fetch_perms flag into the inner plan to prevent reentrant permission checks on cyclic record links"

[[test.results]]
value = "[{ id: node:a, label: 'alpha', next: node:b }, { id: node:b, label: 'beta', next: node:c }, { id: node:c, label: 'gamma', next: node:a }]"

[[test.results]]
value = "[{ id: node:b, label: 'beta', next: node:c }]"

[env]
imports = ["reproductions/skip_fetch_perms_subquery_dereference_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "node:a" }

*/

-- All three nodes should be visible: the permission predicate
-- `WHERE (SELECT VALUE label FROM ONLY next) != NONE` succeeds for each
-- because every node's `next` link points to a valid record with a label.
-- Without the fix this would overflow the stack or time out due to infinite
-- recursion in permission evaluation.
SELECT * FROM node ORDER BY id;

-- Single record lookup should also work without recursion overflow.
SELECT * FROM node:b;
