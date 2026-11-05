/**
[env]
namespace = true
database = true

[test]
reason = """
A LET no statement may hold, written in a closure body, refuses the statement
that writes the closure, wherever the LET stands in the body — after a RETURN,
or in an IF arm that never runs — and the closure is never bound. A closure in
code that never runs is refused the same way. A valid LET after a RETURN is
dead code, and the closure answers its value.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "There was a problem running the ANONYMOUS() function. 'none' is not a function"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "There was a problem running the ANONYMOUS() function. 'none' is not a function"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "There was a problem running the ANONYMOUS() function. 'none' is not a function"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"
*/
LET $f = || { RETURN 1; LET $auth = 2 };
$f();
LET $g = |$x| { IF false { LET $auth = 1 }; $x };
$g(5);
LET $h = || { RETURN 1; SELECT * FROM t WHERE (LET $y = 1) };
$h();
LET $ok = |$x| { RETURN $x; LET $z = 1 };
$ok(7);
{ RETURN 1; LET $dead = || { LET $auth = 2 } };
