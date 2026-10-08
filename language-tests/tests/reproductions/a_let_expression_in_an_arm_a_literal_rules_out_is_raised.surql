/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A `LET` written as an expression is refused as the statement holding it is
planned, so an `IF` arm a literal condition rules out still fails the `IF`
when it holds one: a `false` arm, an `ELSE` after a `true` arm, at the top
level and in a block. A `LET` that is a block's statement, or the value of a
`RETURN` or another `LET`, is no expression, and the arm holding it goes.
Expected values are main's.
"""

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "5"
*/
IF false { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 3 };
IF true { 3 } ELSE { SELECT * FROM t WHERE (LET $x = 1) };
{ IF false { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 3 } };
IF false { [ (LET $x = 1) ] } ELSE { 3 };
IF false { RETURN (LET $x = 1) } ELSE { 3 };
IF false { LET $y = (LET $x = 1) } ELSE { 3 };
IF false { (LET $x = 1) } ELSE { 3 };
IF false { SELECT * FROM t } ELSE { 5 };
