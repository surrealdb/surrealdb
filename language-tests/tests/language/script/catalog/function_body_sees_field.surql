/**
[test]
requires = ["scripting"]
reason = "A statement of a function body after a call that runs an embedded script defining a field applies that field."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "{ f: 42, id: t:9 }"

[[test.results]]
value = "[{ f: 42, id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }, { f: 42, id: t:2 }, { f: 42, id: t:9 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON t VALUE 42") } };
DEFINE FUNCTION fn::y() { fn::x(); RETURN (CREATE ONLY t:9); };
CREATE t:1;
fn::y();
CREATE t:2;
SELECT * FROM t;
