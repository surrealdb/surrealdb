/**
[env]
backend = ["mem", "surrealkv"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A call or a parameter read after a schema statement whose effect depends on
	the data reads the definition the catalog holds when it runs: the body,
	the arguments it declares and coerces, and a parameter's value, also
	through a function that reads it, and a call of a function the block
	removed fails as the catalog says.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: flag:1, on: true }]"

[[test.results]]
value = "20"

[[test.results]]
value = "5"

[[test.results]]
error = "Incorrect arguments for function fn::f(). Failed to coerce argument `$a`: Expected `int` but found `'x'`"

[[test.results]]
value = "[5, 5]"

[[test.results]]
value = "[7, 7]"

[[test.results]]
error = "Function 'fn::f' not found: The function 'fn::f' does not exist"
*/
DEFINE FUNCTION fn::f($a: int) { RETURN $a + 1 };
DEFINE FUNCTION fn::g() { RETURN $p };
CREATE flag:1 SET on = true;
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE FUNCTION OVERWRITE fn::f($a: int) { RETURN $a * 10 } }; RETURN fn::f(2) };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE FUNCTION OVERWRITE fn::f($a: int, $b: int) { RETURN $a + $b } }; RETURN fn::f(2, 3) };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE FUNCTION OVERWRITE fn::f($a: int) { RETURN $a } }; RETURN fn::f('x') };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE PARAM $p VALUE 5 }; RETURN [$p, fn::g()] };
{ FOR $i IN [6, 7] { DEFINE PARAM OVERWRITE $p VALUE $i }; RETURN [$p, fn::g()] };
{ IF (SELECT VALUE on FROM ONLY flag:1) { REMOVE FUNCTION fn::f }; RETURN fn::f(1) };
