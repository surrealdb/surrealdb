/**
[test]
reason = "Test EXPLAIN with LET statement"

[[test.results]]
value = """[
'Let [ctx: Rt] [name: $x]
└────> Expr [ctx: Rt] [expr: 5]
'
]"""

*/

-- EXPLAIN with LET statement
EXPLAIN LET $x = 5;
