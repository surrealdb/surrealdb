/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A user-defined function that calls itself, two that call one another, and a function whose arguments and body call others all return their values, in a statement and in a block that defines a function and calls it."

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
value = "NONE"

[[test.results]]
value = "120"

[[test.results]]
value = "[true, false, false]"

[[test.results]]
value = "10"

[[test.results]]
value = "32"

[[test.results]]
value = "[6, 24, 12]"

[[test.results]]
value = "[5, 7]"

[[test.results]]
value = "[6, 15]"

[[test.results]]
value = "6"
*/

DEFINE FUNCTION fn::fact($n: int) -> int { IF $n <= 1 { RETURN 1 }; RETURN $n * fn::fact($n - 1) };
DEFINE FUNCTION fn::even($n: int) { IF $n = 0 { RETURN true }; RETURN fn::odd($n - 1) };
DEFINE FUNCTION fn::odd($n: int) { IF $n = 0 { RETURN false }; RETURN fn::even($n - 1) };
DEFINE FUNCTION fn::sum_to($n: int) { IF $n = 0 { RETURN 0 }; RETURN $n + (SELECT VALUE fn::sum_to($n - 1) FROM ONLY [1] LIMIT 1) };
DEFINE FUNCTION fn::twice($x: int) { LET $y = $x; RETURN $y * 2 };
DEFINE FUNCTION fn::both($x: int) { LET $t = fn::twice($x); RETURN fn::fact($t) + fn::twice(fn::twice($x)) };
fn::fact(5);
[fn::even(6), fn::odd(6), fn::even(7)];
fn::sum_to(4);
fn::both(2);
[fn::fact(3), fn::fact(4), fn::twice(fn::fact(3))];
{ DEFINE FUNCTION fn::made($x: int) { LET $d = fn::twice($x); RETURN $d + 1 }; RETURN [fn::made(2), fn::made(fn::made(1))] };
{ DEFINE FUNCTION OVERWRITE fn::twice($x: int) { LET $y = $x; RETURN $y * 3 }; RETURN [fn::twice(2), fn::both(1)] };
fn::twice(2);
