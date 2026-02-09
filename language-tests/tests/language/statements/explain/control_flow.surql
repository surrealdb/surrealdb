/**
[test]
reason = "Test EXPLAIN with control flow statements (RETURN, THROW, BREAK, CONTINUE)"

[[test.results]]
value = '''"Return [ctx: Rt]\n└────> Expr [ctx: Rt] [expr: 'OK']\n"'''

[[test.results]]
value = ''''Return [ctx: Rt]\n└────> Expr [ctx: Rt] [expr: 1 + 2]\n''''

[[test.results]]
value = '''"Expr [ctx: Rt] [expr: THROW 'error message']\n"'''

[[test.results]]
value = ''''Expr [ctx: Rt] [expr: BREAK]\n''''

[[test.results]]
value = ''''Expr [ctx: Rt] [expr: CONTINUE]\n''''

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
