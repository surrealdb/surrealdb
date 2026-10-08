/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An `IF` branch or condition written as a `LET`, rather than as a block holding
one, is a `LET` written as an expression. It is refused as the statement
holding the `IF` is planned, whether or not the branch is taken: in a `THEN`,
`ELSE` or `ELSE IF` branch, in a condition, in an `IF` nested in a branch, under
`RETURN` or `LET`, inside an array or a `WHERE`, and in a block. In a block
after a schema statement the `IF` is planned when the block reaches it, and the
refusal is raised only when the branch is taken. A branch that is a block
holding a `LET` runs it. Expected values are main's.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

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
value = "5"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"
*/

LET $c = false;
LET $t = true;
RETURN IF $c THEN (LET $x = 1) ELSE 3 END;
RETURN IF false THEN (LET $x = 1) ELSE 3 END;
IF $c THEN (LET $x = 1) ELSE 3 END;
IF $c THEN 1 ELSE (LET $x = 1) END;
IF $t THEN 1 ELSE IF $c THEN (LET $x = 1) ELSE 3 END;
IF (LET $x = 1) THEN 2 ELSE 3 END;
LET $y = IF $c THEN (LET $x = 1) ELSE 3 END;
RETURN IF $c THEN (IF $t THEN (LET $x = 1) END) ELSE 3 END;
RETURN [IF $c THEN (LET $x = 1) ELSE 3 END];
SELECT * FROM [1] WHERE IF $c THEN (LET $x = 1) ELSE true END;
{ LET $d = false; RETURN IF $d THEN (LET $x = 1) ELSE 3 END };
{ LET $d = false; IF $d { RETURN IF $d THEN (LET $x = 1) ELSE 3 END }; RETURN 5 };
{ DEFINE TABLE late_a; LET $d = false; RETURN IF $d THEN (LET $x = 1) ELSE 3 END };
{ DEFINE TABLE late_b; LET $d = false; IF $d { RETURN IF $d THEN (LET $x = 1) ELSE 3 END }; RETURN 5 };
{ DEFINE TABLE late_c; LET $d = true; RETURN IF $d THEN (LET $x = 1) ELSE 3 END };
IF $c { (LET $x = 1) } ELSE { 3 };
IF $t { LET $x = 1; } ELSE { 3 };
IF $t { LET $x = 1; RETURN $x + 1 } ELSE { 3 };
