/**
[test]
reason = "Test EXPLAIN with control flow statements (RETURN, THROW, BREAK, CONTINUE)"

[[test.results]]
value = '''
"Return [ctx: Rt]
└────> Expr [ctx: Rt] [expr: 'OK']
"'''

[[test.results]]
value = '''
'Return [ctx: Rt]
└────> Expr [ctx: Rt] [expr: 1 + 2]
''''

[[test.results]]
value = '''
"Expr [ctx: Rt] [expr: THROW 'error message']
"'''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: BREAK]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: CONTINUE]
''''

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
