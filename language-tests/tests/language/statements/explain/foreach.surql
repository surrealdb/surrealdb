/**
[test]
reason = "Test EXPLAIN with FOR loop statements"

[[test.results]]
# EXPLAIN FOR loop over array
value = """\
"Foreach [ctx: Rt] [param: item, statements: 1]
""""

[[test.results]]
# EXPLAIN FOR loop over range
value = """\
"Foreach [ctx: Rt] [param: i, statements: 1]
""""

[[test.results]]
# EXPLAIN FOR loop with multiple statements
value = """\
"Foreach [ctx: Rt] [param: x, statements: 2]
""""

[[test.results]]
# EXPLAIN FOR loop with empty body
value = """\
"Foreach [ctx: Rt] [param: n, statements: 0]
""""

*/

-- EXPLAIN FOR loop over array
EXPLAIN FOR $item IN [1, 2, 3] { $item };

-- EXPLAIN FOR loop over range
EXPLAIN FOR $i IN 0..5 { $i };

-- EXPLAIN FOR loop with multiple statements
EXPLAIN FOR $x IN [1, 2] { LET $y = $x; $y };

-- EXPLAIN FOR loop with empty body
EXPLAIN FOR $n IN [1] { };
