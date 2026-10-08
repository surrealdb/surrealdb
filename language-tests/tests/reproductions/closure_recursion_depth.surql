/**
[env]
namespace = true
database = true
# The recursion descends to `max_computation_depth` before erroring, which is
# instant on `mem`; the `eval` case parses and plans a statement per level, so
# the disk backends add only time, not coverage.
backend = ["mem"]

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
A compiled closure that reaches itself again — passed as its own argument and
called directly, through a block of its own, through a closure-taking builtin
such as `array::map`, or through a statement run inside
`eval::surql` — consumes one unit of the computation-depth budget per call and
fails with the computation-depth error once the budget is spent, instead of
growing the native stack without bound. A recursion that terminates within the
budget still returns its value.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"
*/

LET $f = |$s, $n| IF $n = 0 { 0 } ELSE { $s($s, $n - 1) };
RETURN $f($f, 3000);
RETURN $f($f, 3);
LET $g = |$s| array::map([0], |$i| $s($s));
RETURN $g($g);
LET $h = |$s| { RETURN $s($s); };
RETURN $h($h);
LET $e = |$s| eval::surql("CREATE ONLY t SET n = $s($s) RETURN VALUE n", { s: $s });
RETURN $e($e);
