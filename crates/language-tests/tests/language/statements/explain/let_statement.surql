/**
[test]
reason = "Test EXPLAIN with LET statement"

[[test.results]]
value = """
'Let [name: $x]
=> Literal
'"""

*/

-- EXPLAIN with LET statement
EXPLAIN LET $x = 5;
