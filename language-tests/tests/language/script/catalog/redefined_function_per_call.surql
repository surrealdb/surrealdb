/**
[test]
requires = ["scripting"]
reason = "A function whose body an embedded script redefines is called with the new body by each later call: later in the same row, in later rows, in a function body and in a FOR body."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1, w: NONE }, { a: 2, b: 2, id: t:2, w: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1 }, { a: 2, b: 2, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, v: 2 }, { i: 2, v: 2 }]"
*/
DEFINE FUNCTION fn::f() { LET $a = 1; RETURN $a; };
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { LET $a = 2; RETURN $a; }") } };
DEFINE FUNCTION fn::r() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { LET $a = 1; RETURN $a; }") } };
CREATE t:1, t:2;
SELECT id, fn::f() AS a, fn::w() AS w, fn::f() AS b FROM t;
fn::r();
UPDATE t SET a = fn::f(), w = fn::w(), b = fn::f();
fn::r();
DEFINE FUNCTION fn::g() { fn::w(); CREATE k:1 SET v = fn::f(); RETURN [fn::f(), (SELECT VALUE v FROM ONLY k:1)]; };
fn::g();
fn::r();
DEFINE FUNCTION fn::h() { fn::w(); RETURN fn::f(); };
fn::h();
fn::r();
FOR $i IN [1, 2] { fn::w(); CREATE k SET v = fn::f(), i = $i; };
SELECT i, v FROM k WHERE i ORDER BY i;
