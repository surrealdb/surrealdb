/**
[test]
reason = "Test EXPLAIN with closure expressions"

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: any| { $x * 2 }]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: int| { $x * 2 }]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$x: int| -> int { $x * 2 }]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: |$a: int, $b: int| -> int { $a + $b }]
''''

[[test.results]]
value = '''
'Expr [ctx: Rt] [expr: || { 42 }]
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
