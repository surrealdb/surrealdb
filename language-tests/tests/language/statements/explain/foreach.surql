/**
[env]

[test]
reason = "Test EXPLAIN with FOR loop statements"

[[test.results]]
value = '''
'Foreach [ctx: Db] [param: $item]
    ExprPlan [ctx: Rt] [expr: [1, 2, 3]]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: $item]
''''

[[test.results]]
value = '''
'Foreach [ctx: Db] [param: $i]
    ExprPlan [ctx: Rt] [expr: 0..5]
    Sequence [ctx: Rt] [statements: 1]
        ExprPlan [ctx: Rt] [expr: $i]
''''

[[test.results]]
value = '''
'Foreach [ctx: Db] [param: $x]
    ExprPlan [ctx: Rt] [expr: [1, 2]]
    Sequence [ctx: Rt] [statements: 2]
        Let [ctx: Rt] [name: $y]
            ExprPlan [ctx: Rt] [expr: $x]
        ExprPlan [ctx: Rt] [expr: $y]
''''

[[test.results]]
value = '''
'Foreach [ctx: Db] [param: $n]
    ExprPlan [ctx: Rt] [expr: [1]]
    Sequence [ctx: Rt] [statements: 0]
''''

*/

-- EXPLAIN FOR loop over array
EXPLAIN FOR $item IN [1, 2, 3] { $item };

-- EXPLAIN FOR loop over range
EXPLAIN FOR $i IN 0..5 { $i };

-- EXPLAIN FOR loop with multiple statements
EXPLAIN FOR $x IN [1, 2] { LET $y = $x; $y };

-- EXPLAIN FOR loop with empty body
EXPLAIN FOR $n IN [1] { };
