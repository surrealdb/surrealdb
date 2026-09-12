/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test EXPLAIN with IF/ELSE statements"

[[test.results]]
# EXPLAIN simple IF statement
value = """\
"Expr [ctx: Rt] [expr: IF true THEN (Expr) END]
""""

[[test.results]]
# EXPLAIN IF/ELSE statement
value = """\
"Expr [ctx: Rt] [expr: IF true THEN (Expr) ELSE (Expr) END]
""""

[[test.results]]
# EXPLAIN IF/ELSE IF/ELSE statement
value = """\
"Expr [ctx: Rt] [expr: IF $x > 10 THEN (Expr) ELSE IF $x > 5 THEN (Expr) ELSE (Expr) END]
""""

[[test.results]]
# EXPLAIN IF/ELSE IF (no final else)
value = """\
"Expr [ctx: Rt] [expr: IF $x > 10 THEN (Expr) ELSE IF $x > 5 THEN (Expr) END]
""""

*/

-- EXPLAIN simple IF statement
EXPLAIN IF true { "yes" };

-- EXPLAIN IF/ELSE statement
EXPLAIN IF true { "yes" } ELSE { "no" };

-- EXPLAIN IF/ELSE IF/ELSE statement
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" } ELSE { "small" };

-- EXPLAIN IF/ELSE IF (no final else)
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" };
