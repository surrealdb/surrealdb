/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "GROUP BY `in` on edge tables should work - in is a field name on edges"

[[test.results]]
value = "[{ id: person:alice, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: org:acme, name: 'ACME' }]"

[[test.results]]
value = "[{ id: org:globex, name: 'Globex' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ in: person:alice, orgs: 2 }, { in: person:bob, orgs: 1 }]"

[[test.results]]
value = "[{ orgs: 2, person_name: ['Alice', 'Alice'] }, { orgs: 1, person_name: ['Bob'] }]"
*/

-- Setup: create nodes and edges, suppress output for RELATE (random IDs)
CREATE person:alice SET name = "Alice";
CREATE person:bob SET name = "Bob";
CREATE org:acme SET name = "ACME";
CREATE org:globex SET name = "Globex";
LET $_ = (RELATE person:alice->leads->org:acme);
LET $_ = (RELATE person:alice->leads->org:globex);
LET $_ = (RELATE person:bob->leads->org:acme);

-- Test: GROUP BY `in` with backtick escaping
SELECT `in`, count() AS orgs FROM leads GROUP BY `in`;

-- Test: GROUP BY `in` with sub-path SELECT (prefix validation)
SELECT `in`.name AS person_name, count() AS orgs FROM leads GROUP BY `in`;
