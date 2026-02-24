/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test writeable subqueries with UPSERT in LET and SELECT VALUE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tester:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tester:test]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "tester:test"
*/

LET $id = (UPSERT tester:test);
RETURN $id;
LET $id = (UPSERT tester:test).id;
RETURN $id;
LET $id = (SELECT VALUE id FROM (UPSERT tester:test))[0];
RETURN $id;
