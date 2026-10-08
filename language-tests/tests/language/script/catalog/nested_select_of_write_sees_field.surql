/**
[test]
reason = "A SELECT nested in a write's SET is planned when it runs: a computed field an embedded script defines earlier in the row is computed by it, and a REMOVE FIELD before the statement leaves it uncomputed."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 10, id: t:1 }, { a: 10, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 10, id: t:1, k: NONE, r: NONE }, { c: 10, id: t:2, k: NONE, r: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: NONE, id: t:1, r: NONE }, { c: NONE, id: t:2, r: NONE }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE c ON u COMPUTED n * 10") } };
CREATE u:1 SET n = 1;
CREATE t:1, t:2;
UPDATE t SET r = fn::x(), a = (SELECT VALUE c FROM ONLY u:1);
REMOVE FIELD c ON u;
SELECT id, fn::x() AS r, (SELECT VALUE c FROM ONLY u:1) AS c, (CREATE ONLY k RETURN NONE) AS k FROM t;
REMOVE FIELD c ON u;
SELECT id, fn::x() AS r, (SELECT VALUE c FROM ONLY u:1) AS c FROM t;
