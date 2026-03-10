/**
[test]
reason = "Conditional SELECT permissions that follow RecordId links should not recurse infinitely on cyclic data, and computed fields on linked records should be evaluated"

[[test.results]]
value = "[{ age: 25, can_drive: true, friend: person:b, id: person:a }, { age: 30, can_drive: true, friend: person:a, id: person:b }]"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:a, id: person:b }]"

[env]
imports = ["reproductions/reentrant_permission_check_cyclic_links_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:a" }

*/

-- Both records should be visible: friend.can_drive (computed from age >= 18) is true for each
SELECT * FROM person ORDER BY id;

-- Single record lookup should also work
SELECT * FROM person:b;
