/**
[test]
reason = "A function body and a transaction both reach a function an embedded script redefines: a call made after the redefinition runs the new body."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }") } };
DEFINE FUNCTION fn::g() { LET $a = fn::f(); fn::w(); RETURN [$a, fn::f()]; };
fn::g();
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1 };
BEGIN;
LET $x = fn::f();
fn::w();
LET $y = fn::f();
RETURN [$x, $y];
COMMIT;
