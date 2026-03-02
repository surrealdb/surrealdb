/**
[test]
reason = "Test writeable subqueries with UPSERT in LET and SELECT VALUE (new executor)"
*/

LET $id = (UPSERT tester:test);
RETURN $id;
LET $id = (UPSERT tester:test).id;
RETURN $id;
LET $id = (SELECT VALUE id FROM (UPSERT tester:test))[0];
RETURN $id;
