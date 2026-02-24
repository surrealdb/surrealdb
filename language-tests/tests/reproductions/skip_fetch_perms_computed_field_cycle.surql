/**
[test]
reason = "Computed fields that dereference RecordIds during permission evaluation must preserve the skip_fetch_perms flag to prevent reentrant permission checks on cyclic links"

[[test.results]]
value = "[{ id: node:b, label: 'beta', next: node:c }]"

[[test.results]]
value = "[{ id: node:b, label: 'beta', next: node:c }]"

[env]
imports = ["reproductions/skip_fetch_perms_computed_field_cycle_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "node:a" }

*/

-- All three nodes should be visible: for each, next.tag resolves to a non-NONE
-- label through two levels of RecordId dereference (permission predicate reads
-- next.tag, and tag itself is computed as next.label).
SELECT * FROM node ORDER BY id;

-- Single record lookup should also work without recursion overflow.
SELECT * FROM node:b;
