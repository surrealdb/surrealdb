/**
[test]
reason = "Every kind of write nested in a SELECT is planned when it runs: a field an embedded script defines earlier in the statement applies to the records each writes."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: u:1 }, { id: u:2 }, { id: u:9 }]"

[[test.results]]
value = "[{ a: 42, b: [42], d: [{ id: u:9 }], e: [u:2], id: t:1, r: NONE }, { a: 42, b: [42], d: [], e: [u:2], id: t:2, r: NONE }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON u VALUE 42") } };
CREATE t:1, t:2;
CREATE u:1, u:2, u:9;
SELECT id, fn::x() AS r, (UPSERT ONLY u:3 RETURN VALUE f) AS a, (INSERT INTO u { id: rand::uuid() } RETURN VALUE f) AS b, (RELATE u:1->e->u:2 RETURN VALUE out) AS e, (DELETE u:9 RETURN BEFORE) AS d FROM t;
