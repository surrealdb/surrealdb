/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test EXPLAIN with FOR loop statements"

[[test.results]]
# EXPLAIN FOR loop over array
value = """\
"Foreach [ctx: Rt] [param: $item, range: [1, 2, 3]]
    Expr [ctx: Rt] [expr: $item]
""""

[[test.results]]
# EXPLAIN FOR loop over range
value = """\
"Foreach [ctx: Rt] [param: $i, range: 0 .. 5]
    Expr [ctx: Rt] [expr: $i]
""""

[[test.results]]
# EXPLAIN FOR loop with multiple statements
value = """\
"Foreach [ctx: Rt] [param: $x, range: [1, 2]]
    Sequence [ctx: Rt] [statements: 2]
        Let [ctx: Rt] [name: $y]
            Expr [ctx: Rt] [expr: $x]
        Expr [ctx: Rt] [expr: $y]
""""

[[test.results]]
# EXPLAIN FOR loop with empty body
value = """\
"Foreach [ctx: Rt] [param: $n, range: [1]]
    Expr [ctx: Rt] [expr: NONE]
""""

*/

-- EXPLAIN FOR loop over array
EXPLAIN FOR $item IN [1, 2, 3] { $item };

-- EXPLAIN FOR loop over range
EXPLAIN FOR $i IN 0..5 { $i };

-- EXPLAIN FOR loop with multiple statements
EXPLAIN FOR $x IN [1, 2] { LET $y = $x; $y };

-- EXPLAIN FOR loop with empty body
EXPLAIN FOR $n IN [1] { };
