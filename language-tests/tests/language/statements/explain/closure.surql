/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test EXPLAIN with closure expressions"

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: any| (
  Sequence
    Eval[$x * 2]
)]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: int| (
  Sequence
    Eval[$x * 2]
)]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: int| -> int (
  Sequence
    Eval[$x * 2]
)]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$a: int, $b: int| -> int (
  Sequence
    Eval[$a + $b]
)]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: || (
  Sequence
    Eval[42]
)]
''''

*/

-- EXPLAIN basic closure
EXPLAIN |$x| { $x * 2 };

-- EXPLAIN closure with typed argument
EXPLAIN |$x: int| { $x * 2 };

-- EXPLAIN closure with return type
EXPLAIN |$x: int| -> int { $x * 2 };

-- EXPLAIN closure with multiple arguments
EXPLAIN |$a: int, $b: int| -> int { $a + $b };

-- EXPLAIN closure with no arguments
EXPLAIN || { 42 };
