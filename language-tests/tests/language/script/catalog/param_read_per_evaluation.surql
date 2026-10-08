/**
[test]
reason = "A parameter an embedded script redefines is read as the catalog stands where it is read: by later rows of the same statement, and by later statements of a function body."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1, w: NONE }, { a: 2, b: 2, id: t:2, w: NONE }, { a: 2, b: 2, id: t:3, w: NONE }]"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"
*/
DEFINE PARAM $p VALUE 1;
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE PARAM OVERWRITE $p VALUE 2") } };
CREATE t:1, t:2, t:3;
SELECT id, $p AS a, fn::w() AS w, $p AS b FROM t;
RETURN $p;
DEFINE PARAM OVERWRITE $p VALUE 1;
DEFINE FUNCTION fn::g() { LET $a = $p; fn::w(); RETURN [$a, $p]; };
fn::g();
