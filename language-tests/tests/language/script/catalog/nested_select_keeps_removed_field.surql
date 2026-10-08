/**
[test]
reason = "A SELECT nested in a SELECT's projection is planned with the statement: a computed field an embedded script removes while the statement runs is still computed by the nested SELECT."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 10, id: u:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:1, l: u:1 }, { id: t:2, l: u:1 }, { id: t:3, l: u:1 }]"

[[test.results]]
value = "[{ c: NONE, d: 10, id: t:1, r: NONE }, { c: NONE, d: 10, id: t:2, r: NONE }, { c: NONE, d: 10, id: t:3, r: NONE }]"
*/
DEFINE FUNCTION fn::r() { RETURN function() { return surrealdb.query("REMOVE FIELD IF EXISTS c ON u") } };
DEFINE FIELD c ON u COMPUTED n * 10;
CREATE u:1 SET n = 1;
CREATE t:1, t:2, t:3 SET l = u:1;
SELECT id, fn::r() AS r, l.c AS c, (SELECT VALUE c FROM ONLY u:1) AS d FROM t;
