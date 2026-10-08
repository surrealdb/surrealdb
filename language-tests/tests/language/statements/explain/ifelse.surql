/**
[env]

[test]
reason = "Test EXPLAIN with IF/ELSE statements"

[[test.results]]
value = '''
"Sequence [ctx: Rt] [statements: 1]
    ExprPlan [ctx: Rt] [expr: 'yes']
"'''

[[test.results]]
value = '''
"Sequence [ctx: Rt] [statements: 1]
    ExprPlan [ctx: Rt] [expr: 'yes']
"'''

[[test.results]]
value = '''
"If [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: $x > 10]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: 'large']
    ExprPlan [ctx: Rt] [expr: $x > 5]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: 'medium']
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: 'small']
"'''

[[test.results]]
value = '''
"If [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: $x > 10]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: 'large']
    ExprPlan [ctx: Rt] [expr: $x > 5]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: 'medium']
"'''

*/

-- EXPLAIN simple IF statement
EXPLAIN IF true { "yes" };

-- EXPLAIN IF/ELSE statement
EXPLAIN IF true { "yes" } ELSE { "no" };

-- EXPLAIN IF/ELSE IF/ELSE statement
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" } ELSE { "small" };

-- EXPLAIN IF/ELSE IF (no final else)
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" };
