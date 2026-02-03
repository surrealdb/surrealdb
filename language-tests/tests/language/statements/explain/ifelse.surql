/**
[test]
reason = "Test EXPLAIN with IF/ELSE statements"

[[test.results]]
# EXPLAIN simple IF statement
value = """\
"IfElse [ctx: Db] [branches: 1]
""""

[[test.results]]
# EXPLAIN IF/ELSE statement
value = """\
"IfElse [ctx: Db] [branches: 1, has_else: true]
""""

[[test.results]]
# EXPLAIN IF/ELSE IF/ELSE statement
value = """\
"IfElse [ctx: Db] [branches: 2, has_else: true]
""""

[[test.results]]
# EXPLAIN IF/ELSE IF (no final else)
value = """\
"IfElse [ctx: Db] [branches: 2]
""""

*/

-- EXPLAIN simple IF statement
EXPLAIN IF true { "yes" };

-- EXPLAIN IF/ELSE statement
EXPLAIN IF true { "yes" } ELSE { "no" };

-- EXPLAIN IF/ELSE IF/ELSE statement
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" } ELSE { "small" };

-- EXPLAIN IF/ELSE IF (no final else)
EXPLAIN IF $x > 10 { "large" } ELSE IF $x > 5 { "medium" };
