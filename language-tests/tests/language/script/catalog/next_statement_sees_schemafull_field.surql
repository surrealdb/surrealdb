/**
[env]
compatibility = 3

[test]
reason = "A schemafull table's field an embedded script defines or overwrites types the writes after the call."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, b: 'y', id: t:2 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, b: 'y', id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `a` of `t:3`: Expected `string` but found `3`"

[[test.results]]
value = "[{ a: 'z', b: 'x', id: t:4 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, b: 'y', id: t:2 }, { a: 'z', b: 'x', id: t:4 }]"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE int;
CREATE t:1 SET a = 1;
(function() { return surrealdb.query("DEFINE FIELD b ON t TYPE string DEFAULT 'x'") });
CREATE t:2 SET a = 2, b = 'y';
SELECT * FROM t;
(function() { return surrealdb.query("DEFINE FIELD OVERWRITE a ON t TYPE string") });
CREATE t:3 SET a = 3;
CREATE t:4 SET a = 'z';
SELECT * FROM t;
