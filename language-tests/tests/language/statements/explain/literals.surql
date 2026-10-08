/**
[env]

[test]
reason = "Test EXPLAIN with literals"

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: 9] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Sequence [ctx: Rt] [statements: 1]
        Return [ctx: Rt]
            ExprPlan [ctx: Rt] [expr: 9]
''''

[[test.results]]
value = '''
'Return [ctx: Rt] {rows: 0}
    Sequence [ctx: Rt] [statements: 1] {rows: 0}
        Return [ctx: Rt] {rows: 0}
            ExprPlan [ctx: Rt] [expr: 9] {rows: 0}

Total rows: 0''''

*/

EXPLAIN ANALYZE 9;
EXPLAIN RETURN IF true { RETURN 9 } ELSE { 10 };
EXPLAIN ANALYZE RETURN IF true { RETURN 9 } ELSE { 10 };
