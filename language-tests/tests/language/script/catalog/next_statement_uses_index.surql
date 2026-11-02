/**
[test]
requires = ["scripting"]
reason = "An index, and a unique index, an embedded script defines serve and constrain the statements after the call."

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 1, id: t:3 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:3`"
*/
CREATE t:1 SET a = 1;
(function() { return surrealdb.query("DEFINE INDEX i ON t FIELDS a") });
CREATE t:2 SET a = 2;
SELECT * FROM t WHERE a = 2;
SELECT * FROM t WHERE a = 1;
(function() { return surrealdb.query("DEFINE INDEX u ON t FIELDS b UNIQUE") });
CREATE t:3 SET b = 1;
CREATE t:4 SET b = 1;
