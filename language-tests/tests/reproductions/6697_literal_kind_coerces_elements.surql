/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A literal kind converts its members to the member kinds as `array<int>` converts its elements: `[1.0]` coerced to `[int]` is `[1]`, a tuple literal converts each position, and an object literal converts each declared key. Members that are already of the member kind, and keys the value omits, are untouched.
"""
issue = 6697

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "'int'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'int'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'int'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'int'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 2 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[1]"

*/
LET $v: [int] = [1.0];
$v;
type::of($v[0]);
LET $w: array<int> = [1.0];
type::of($w[0]);
LET $x: [int, string] = [1.0, 's'];
type::of($x[0]);
LET $o: { a: int } = { a: 1.0 };
type::of($o.a);
LET $p: { a: int, b: option<float> } = { a: 2.0 };
$p;
LET $u: [int] | [string] = [3.0];
$u;
RETURN <[int]> [1.0];
