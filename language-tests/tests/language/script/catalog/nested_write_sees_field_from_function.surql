/**
[test]
reason = "A write nested in an expression is planned when it runs: a field a function's DEFINE FIELD statement defines earlier in the statement applies to it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, r: NONE, v: 42 }, { id: t:2, r: NONE, v: 42 }]"
*/
DEFINE FUNCTION fn::d() { DEFINE FIELD OVERWRITE f ON ud VALUE 42; };
CREATE t:1, t:2;
SELECT id, fn::d() AS r, (CREATE ONLY ud RETURN VALUE f) AS v FROM t;
