/**
[test]
requires = ["scripting"]
reason = "A statement that runs an embedded script inlines no function body: each call reads the definition the transaction holds when it is made, and a function unit that inlined another function is not served once that function is redefined or removed."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, NONE, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, NONE, 20]"

[[test.results]]
value = "20"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::f' not found: The function 'fn::f' does not exist"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }") } };
RETURN [fn::f(), fn::w(), fn::f()];
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1 };
DEFINE FUNCTION fn::h() { RETURN fn::f() * 10 };
RETURN [fn::h(), fn::w(), fn::h()];
fn::h();
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1 };
DEFINE FUNCTION fn::w2() { RETURN function() { return surrealdb.query("REMOVE FUNCTION fn::f") } };
RETURN [fn::f(), fn::w2(), fn::f()];
