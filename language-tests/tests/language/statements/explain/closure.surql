/**
[test]
reason = "Test EXPLAIN with closure expressions"

[[test.results]]
# EXPLAIN basic closure
value = """['Closure [ctx: Rt] [args: 1]
']"""

[[test.results]]
# EXPLAIN closure with typed argument
value = """['Closure [ctx: Rt] [args: 1]
']"""

[[test.results]]
# EXPLAIN closure with return type
value = """['Closure [ctx: Rt] [args: 1, has_return_type: true]
']"""

[[test.results]]
# EXPLAIN closure with multiple arguments
value = """['Closure [ctx: Rt] [args: 2, has_return_type: true]
']"""

[[test.results]]
# EXPLAIN closure with no arguments
value = """['Closure [ctx: Rt] [args: 0]
']"""

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
