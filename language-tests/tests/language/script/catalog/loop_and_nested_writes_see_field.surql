/**
[test]
requires = ["scripting"]
reason = "A FOR body and a write nested in a write each apply a field an embedded script defines before them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, n: 1 }, { f: 42, n: 1 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "NONE"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON u VALUE 42") } };
CREATE t:1, t:2;
FOR $r IN (SELECT * FROM t) { fn::x(); CREATE u SET n = 1; };
SELECT n, f FROM u;
UPDATE t SET v = (CREATE ONLY w SET n = fn::x() RETURN VALUE n);
LET $q = (SELECT VALUE [fn::x(), (CREATE ONLY u2 RETURN VALUE id)] FROM t);
