/**
[test]
reason = "Conditional SELECT permissions that follow RecordId links should not recurse infinitely on cyclic data"

[[test.results]]
value = "[{ age: 25, friend: person:b, id: person:a }, { age: 30, friend: person:a, id: person:b }]"

[[test.results]]
value = "[{ age: 30, friend: person:a, id: person:b }]"

[env]
imports = ["reproductions/reentrant_permission_check_cyclic_links_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:a" }

*/

-- Both records should be visible: friend.age > 0 holds for each
SELECT * FROM person ORDER BY id;

-- Single record lookup should also work
SELECT * FROM person:b;
