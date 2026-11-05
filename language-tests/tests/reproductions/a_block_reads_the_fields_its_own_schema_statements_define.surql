/**
[env]
backend = ["mem", "surrealkv"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement after a schema statement in the same block reads the catalog
	that schema statement leaves, also on a table an earlier statement already
	gave computed fields: a record read, a projection of the new field, a
	nested select and a table scan in the block each see the field the block
	defined. So does a loop iteration after an earlier iteration's schema
	statement.
"""

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ a: 1, id: t:1, n: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1, n: 1 }]"

[[test.results]]
value = "[{ b: 2, c: 3 }]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, d: 4, e: 5, id: t:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, id: t:1, n: 1 }]"

[[test.results]]
value = "[8]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, v: 0 }, { i: 2, v: 9 }, { i: 3, v: 9 }]"
*/
CREATE t:1 SET n = 1;
{ DEFINE FIELD a ON t COMPUTED 1; RETURN SELECT * FROM t:1 };
{ DEFINE FIELD b ON t COMPUTED 2; RETURN SELECT * FROM t:1 };
{ DEFINE FIELD c ON t COMPUTED 3; RETURN SELECT b, c FROM t:1 };
{ DEFINE FIELD d ON t COMPUTED 4; RETURN (SELECT * FROM t:1).d };
{ DEFINE FIELD e ON t COMPUTED 5; RETURN SELECT * FROM t WHERE true };
DEFINE FIELD f ON t COMPUTED 6;
{ DEFINE FIELD g ON t COMPUTED 7; RETURN SELECT * FROM t:1 };
{ DEFINE FIELD h ON t COMPUTED 8; LET $r = SELECT VALUE h FROM t:1; RETURN $r };
DEFINE FIELD k ON t COMPUTED 0;
FOR $i IN [1, 2, 3] { IF $i = 2 { DEFINE FIELD OVERWRITE k ON t COMPUTED 9 }; CREATE u SET i = $i, v = (SELECT VALUE k FROM ONLY t:1) };
SELECT i, v FROM u ORDER BY i;
