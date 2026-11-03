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
    ExprPlan [ctx: Rt] [expr: IF true THEN (
  Sequence
    Return
      Eval[9]
) ELSE (
  Sequence
    Eval[10]
) END]
''''

[[test.results]]
value = '''
'Return [ctx: Rt] {rows: 0}
    ExprPlan [ctx: Rt] [expr: IF true THEN (
  Sequence
    Return
      Eval[9]
) ELSE (
  Sequence
    Eval[10]
) END] {rows: 0}

Total rows: 0''''

*/

EXPLAIN ANALYZE 9;
EXPLAIN RETURN IF true { RETURN 9 } ELSE { 10 };
EXPLAIN ANALYZE RETURN IF true { RETURN 9 } ELSE { 10 };
