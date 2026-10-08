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
	Plan companion to closure_recursion_depth: a RETURN of a closure
	call plans as a Return above the call expression, and the closure body is
	never unfolded into the plan, whichever way the closure reaches itself
	(directly, through a block of its own, through a closure-taking builtin,
	or through an `eval::surql` statement). EXPLAIN plans the call without
	invoking it, so a recursion that would exhaust the computation-depth
	budget when run still yields a plan.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: ($f)($f, 3000)]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: ($f)($f, 3)]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: ($g)($g)]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: ($h)($h)]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: ($e)($e)]
''''
*/

LET $f = |$s, $n| IF $n = 0 { 0 } ELSE { $s($s, $n - 1) };
EXPLAIN RETURN $f($f, 3000);
EXPLAIN RETURN $f($f, 3);
LET $g = |$s| array::map([0], |$i| $s($s));
EXPLAIN RETURN $g($g);
LET $h = |$s| { RETURN $s($s); };
EXPLAIN RETURN $h($h);
LET $e = |$s| eval::surql("CREATE ONLY t SET n = $s($s) RETURN VALUE n", { s: $s });
EXPLAIN RETURN $e($e);
