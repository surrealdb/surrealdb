/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A call of a function whose body may run in the caller returns what the call
	returns: a declared kind that would convert or refuse an argument keeps the
	call, an omitted optional argument is NONE, an argument that may raise is
	still evaluated, and a function a statement redefines before the call is
	read as it then stands.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1, s: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"

[[test.results]]
error = "Incorrect arguments for function fn::double(). Failed to coerce argument `$x`: Expected `int` but found `'a'`"

[[test.results]]
value = "[[1, NONE]]"

[[test.results]]
error = "Incorrect arguments for function fn::pad(). The function expects 1 to 2 arguments."

[[test.results]]
error = "Could not cast into `int` using input `'a'`"

[[test.results]]
error = "Couldn't coerce return value from function `fn::text`: Expected `string` but found `2`"

[[test.results]]
value = "6"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET n = 1, s = 'a';
DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2 };
DEFINE FUNCTION fn::pad($x: int, $y: option<int>) { [$x, $y] };
DEFINE FUNCTION fn::one($x: any) { RETURN 1 };
DEFINE FUNCTION fn::text($x: int) -> string { RETURN $x * 2 };
SELECT VALUE fn::double(n) FROM t;
SELECT VALUE fn::double(s) FROM t;
SELECT VALUE fn::pad(n) FROM t;
SELECT VALUE fn::pad() FROM t;
SELECT VALUE fn::one(<int> s) FROM t;
SELECT VALUE fn::text(n) FROM t;
{ DEFINE FUNCTION OVERWRITE fn::double($x: int) { RETURN $x * 3 }; RETURN fn::double(2) };
