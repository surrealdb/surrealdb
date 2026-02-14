/**
[test]
reason = "Test EXPLAIN with literals"

[[test.results]]
value = ''''Expr [ctx: Rt] [expr: 9] {rows: 0, batches: 0}

Total rows: 1''''

[[test.results]]
value = ''''Return [ctx: Rt]
└────> IfElse [ctx: Rt] [branches: 1, has_else: true]
''''

[[test.results]]
value = ''''Return [ctx: Rt] {rows: 0, batches: 0}
└────> IfElse [ctx: Rt] [branches: 1, has_else: true] {rows: 0, batches: 0}

Total rows: 0''''

*/

EXPLAIN ANALYZE 9;
EXPLAIN RETURN IF true { RETURN 9 } ELSE { 10 };
EXPLAIN ANALYZE RETURN IF true { RETURN 9 } ELSE { 10 };
