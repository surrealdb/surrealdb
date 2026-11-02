/**
[test]
requires = ["scripting"]
reason = "A field an embedded script defines through surrealdb.query() is applied by the statements after the call, in later auto-commit transactions."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, id: t:2 }]"

[[test.results]]
value = "[{ f: 42, id: t:1 }]"

[[test.results]]
value = "[{ f: 42, id: t:1 }, { f: 42, id: t:2 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD f ON t VALUE 42") } };
CREATE t:1;
fn::x();
CREATE t:2;
UPDATE t:1;
SELECT * FROM t;
