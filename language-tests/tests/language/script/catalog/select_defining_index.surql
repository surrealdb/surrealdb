/**
[test]
reason = "An index an embedded script defines while a SELECT runs changes none of the SELECT's rows, and serves the statements after it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, r: NONE }, { id: t:2, r: NONE }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 2, id: t:3 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }, { a: 2, id: t:3 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE INDEX IF NOT EXISTS i ON t FIELDS a") } };
CREATE t:1 SET a = 1; CREATE t:2 SET a = 2;
SELECT id, fn::x() AS r FROM t;
SELECT * FROM t WHERE a = 2;
CREATE t:3 SET a = 2;
SELECT * FROM t WHERE a = 2;
