/**
[test]
reason = "A field an embedded script defines through a closure, or a function a closure calls, is applied by the statements after the call."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, g: 7, id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { f: 42, id: t:2 }, { f: 42, g: 7, id: t:3 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON t VALUE 42") } };
LET $c = || fn::x();
CREATE t:1;
$c();
CREATE t:2;
LET $d = || function() { return surrealdb.query("DEFINE FIELD OVERWRITE g ON t VALUE 7") };
$d();
CREATE t:3;
SELECT * FROM t;
