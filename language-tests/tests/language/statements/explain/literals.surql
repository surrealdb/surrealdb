/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test EXPLAIN with literals"

[[test.results]]
value = ''''Expr [ctx: Rt] [expr: 9] {rows: 0}

Total rows: 1''''

[[test.results]]
value = ''''Return [ctx: Rt]
    Expr [ctx: Rt] [expr: IF true THEN (Return) ELSE (Expr) END]
''''

[[test.results]]
value = ''''Return [ctx: Rt] {rows: 0}
    Expr [ctx: Rt] [expr: IF true THEN (Return) ELSE (Expr) END] {rows: 0}

Total rows: 0''''

*/

EXPLAIN ANALYZE 9;
EXPLAIN RETURN IF true { RETURN 9 } ELSE { 10 };
EXPLAIN ANALYZE RETURN IF true { RETURN 9 } ELSE { 10 };
