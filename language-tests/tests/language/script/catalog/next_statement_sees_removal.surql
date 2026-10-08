/**
[test]
reason = "A field, an event and a table an embedded script removes or defines are seen by the statements after the call."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 10, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, c: 10, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ r: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 't' does not exist"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:3 }]"
*/
DEFINE FIELD c ON t COMPUTED a * 10;
CREATE t:1 SET a = 1;
SELECT * FROM t;
(function() { return surrealdb.query("REMOVE FIELD c ON t") });
SELECT * FROM t;
(function() { return surrealdb.query("DEFINE EVENT e ON t THEN { CREATE log SET r = $after.id }") });
CREATE t:2 SET a = 2;
SELECT r FROM log;
(function() { return surrealdb.query("REMOVE TABLE t") });
SELECT * FROM t;
CREATE t:3;
SELECT * FROM t;
