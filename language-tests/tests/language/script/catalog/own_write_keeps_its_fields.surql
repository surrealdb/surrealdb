/**
[test]
requires = ["scripting"]
reason = "A write loads its table's fields when it starts: a field an embedded script defines while the write runs applies to none of its rows."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON t VALUE 42") } };
CREATE t:1, t:2, t:3;
UPDATE t SET a = fn::x();
SELECT * FROM t;
