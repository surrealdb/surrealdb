/**
[test]
reason = "A SELECT loads its table's fields when it starts: a computed field an embedded script defines while it runs is computed for none of its rows, and for every row of the next statement."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "[{ id: t:3, n: 3 }]"

[[test.results]]
value = "[{ c: NONE, n: 1, r: NONE }, { c: NONE, n: 2, r: NONE }, { c: NONE, n: 3, r: NONE }]"

[[test.results]]
value = "[{ c: 10, n: 1, r: NONE }, { c: 20, n: 2, r: NONE }, { c: 30, n: 3, r: NONE }]"

[[test.results]]
value = "[{ c: 10, id: t:1, n: 1 }, { c: 20, id: t:2, n: 2 }, { c: 30, id: t:3, n: 3 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE c ON t COMPUTED n * 10") } };
CREATE t:1 SET n = 1; CREATE t:2 SET n = 2; CREATE t:3 SET n = 3;
SELECT n, c, fn::x() AS r FROM t;
SELECT n, fn::x() AS r, c FROM t;
SELECT * FROM t;
