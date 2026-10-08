/**
[test]
reason = "A write loads its table's fields when it starts: a field an embedded script removes while it runs still applies to all its rows, and to none of the next write's."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, id: t:1 }, { f: 42, id: t:2 }, { f: 42, id: t:3 }]"

[[test.results]]
value = "[{ f: 42, id: t:1 }, { f: 42, id: t:2 }, { f: 42, id: t:3 }]"

[[test.results]]
value = "[{ f: 42, id: t:1 }, { f: 42, id: t:2 }, { f: 42, id: t:3 }]"

[[test.results]]
value = "[{ f: 1, id: t:1 }, { f: 1, id: t:2 }, { f: 1, id: t:3 }]"

[[test.results]]
value = "[{ f: 1, id: t:1 }, { f: 1, id: t:2 }, { f: 1, id: t:3 }]"
*/
DEFINE FIELD f ON t VALUE 42;
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("REMOVE FIELD IF EXISTS f ON t") } };
CREATE t:1, t:2, t:3;
UPDATE t SET f = 0, a = fn::x();
SELECT * FROM t;
UPDATE t SET f = 1;
SELECT * FROM t;
