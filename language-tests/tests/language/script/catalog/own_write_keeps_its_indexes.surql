/**
[test]
reason = "A write loads its table's indexes when it starts: a unique index an embedded script defines while it runs constrains none of its rows, and constrains the next write."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 1, id: t:2 }, { a: 1, id: t:3 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 1, id: t:2 }, { a: 1, id: t:3 }]"

[[test.results]]
error = "Database index `u` already contains 2, with record `t:1`"

[[test.results]]
value = "[]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE INDEX IF NOT EXISTS u ON t FIELDS a UNIQUE") } };
CREATE t:1, t:2, t:3;
UPDATE t SET a = 1, b = fn::x();
SELECT * FROM t;
UPDATE t SET a = 2;
SELECT * FROM t WHERE a = 1;
