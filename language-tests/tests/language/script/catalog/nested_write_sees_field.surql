/**
[test]
requires = ["scripting"]
reason = "A write nested in an expression is planned when it runs: a field an embedded script defines earlier in the statement applies to the nested write's records, from the first row on."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1, r: NONE, v: { f: 42 } }, { id: t:2, r: NONE, v: { f: 42 } }, { id: t:3, r: NONE, v: { f: 42 } }]"

[[test.results]]
value = "[{ id: t:1, r: NONE, v: { f: NONE } }, { id: t:2, r: NONE, v: { f: 42 } }, { id: t:3, r: NONE, v: { f: 42 } }]"

[[test.results]]
value = "[{ id: uc:1, n: 1 }]"

[[test.results]]
value = "[{ id: s:1, l: uc:1 }, { id: s:2, l: uc:1 }, { id: s:3, l: uc:1 }]"

[[test.results]]
value = "[{ id: s:1, r: NONE, v: { f: 42 } }, { id: s:2, r: NONE, v: { f: 42 } }, { id: s:3, r: NONE, v: { f: 42 } }]"
*/
DEFINE FUNCTION fn::a() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON ua VALUE 42") } };
DEFINE FUNCTION fn::b() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON ub VALUE 42") } };
DEFINE FUNCTION fn::c() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON uc VALUE 42") } };
CREATE t:1, t:2, t:3;
SELECT id, fn::a() AS r, (CREATE ONLY ua CONTENT { n: 1 } RETURN f) AS v FROM t;
SELECT id, (CREATE ONLY ub CONTENT { n: 1 } RETURN f) AS v, fn::b() AS r FROM t;
CREATE uc:1 SET n = 1;
CREATE s:1, s:2, s:3 SET l = uc:1;
SELECT id, fn::c() AS r, (UPDATE ONLY uc:1 RETURN f) AS v FROM s;
