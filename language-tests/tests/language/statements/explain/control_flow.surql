/**
[test]
reason = "Test EXPLAIN with control flow statements (RETURN, THROW, BREAK, CONTINUE)"

[[test.results]]
# EXPLAIN RETURN with simple expression
value = """\
"ControlFlow [ctx: Rt] [kind: RETURN]
└────> Expr [ctx: Rt] [expr: 'OK']
""""

[[test.results]]
# EXPLAIN RETURN with arithmetic expression
value = """\
"ControlFlow [ctx: Rt] [kind: RETURN]
└────> Expr [ctx: Rt] [expr: 1 + 2]
""""

[[test.results]]
# EXPLAIN THROW
value = """\
"ControlFlow [ctx: Rt] [kind: THROW]
└────> Expr [ctx: Rt] [expr: 'error message']
""""

[[test.results]]
# EXPLAIN BREAK
value = """\
"ControlFlow [ctx: Rt] [kind: BREAK]
""""

[[test.results]]
# EXPLAIN CONTINUE
value = """\
"ControlFlow [ctx: Rt] [kind: CONTINUE]
""""

*/

-- EXPLAIN RETURN with simple expression
EXPLAIN RETURN "OK";

-- EXPLAIN RETURN with arithmetic expression
EXPLAIN RETURN 1 + 2;

-- EXPLAIN THROW
EXPLAIN THROW "error message";

-- EXPLAIN BREAK
EXPLAIN BREAK;

-- EXPLAIN CONTINUE
EXPLAIN CONTINUE;
