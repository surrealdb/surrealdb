/**
[test]
requires = ["scripting"]
reason = "Statements nested in a SELECT that also writes are planned when they run: a function an embedded script redefines earlier in the statement is called with its new body by each of them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 2, b: 2, c: 2, id: t:1, w: NONE }, { a: 2, b: 2, c: 2, id: t:2, w: NONE }]"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }") } };
CREATE t:1, t:2;
SELECT id, fn::w() AS w, (RETURN fn::f()) AS a, (SELECT VALUE fn::f() FROM ONLY 1) AS b, (CREATE ONLY k SET v = fn::f() RETURN VALUE v) AS c FROM t;
