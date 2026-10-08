/**
[test]
reason = "A SELECT that also writes plans its nested statements when they run: a computed field an embedded script defines earlier in the statement is computed by its nested SELECT."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ c: 10, f: NONE, id: t:1, r: NONE }, { c: 10, f: NONE, id: t:2, r: NONE }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE c ON u COMPUTED n * 10; DEFINE FIELD OVERWRITE f ON u VALUE 42;") } };
CREATE u:1 SET n = 1;
CREATE t:1, t:2;
SELECT id, fn::x() AS r, (SELECT VALUE c FROM ONLY u:1) AS c, (UPDATE ONLY u:1 SET m = 1 RETURN VALUE f) AS f FROM t;
