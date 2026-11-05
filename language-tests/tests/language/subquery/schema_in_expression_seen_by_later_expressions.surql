/**
[test]
reason = "A schema statement written in an expression is seen by what the statement evaluates after it: later calls of a function it redefines, statements nested after it, and the rows a later statement writes."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1 }, { a: 2, c: 2, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1, w: NONE }, { a: 2, c: 2, id: t:2, w: NONE }]"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1 }, { a: 2, c: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1 }, { a: 2, c: 2, id: t:2 }]"

[[test.results]]
value = "[{ id: u:1, n: 1 }]"

[[test.results]]
value = "[{ a: 10, c: 2, f: 42, id: t:1 }, { a: 10, c: 2, f: 42, id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, r: NONE, v: 42 }, { id: t:2, r: NONE, v: 42 }]"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
CREATE t:1, t:2;
UPDATE t SET b = (DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }), a = fn::f(), c = (RETURN fn::f());
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1 };
SELECT id, (DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }) AS w, fn::f() AS a, (RETURN fn::f()) AS c FROM t;
UPDATE t SET r = (DEFINE FIELD OVERWRITE f ON t VALUE 42);
SELECT * FROM t;
CREATE u:1 SET n = 1;
UPDATE t SET r = (DEFINE FIELD OVERWRITE c ON u COMPUTED n * 10), a = (SELECT VALUE c FROM ONLY u:1);
SELECT id, (DEFINE FIELD OVERWRITE f ON ua VALUE 42) AS r, (CREATE ONLY ua RETURN VALUE f) AS v FROM t;
