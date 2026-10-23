/**
[env]
namespace = true
database = true

[test]
reason = """
Of two LETs no statement may hold, the statement is refused with the error of
the one written first: an expression and the statements nested in it are
checked before the next expression, so a LET written later never displaces an
earlier one's error. A statement IF is checked a condition and its arm at a
time. A FOR's range and a SLEEP's duration are expressions, refused even where
the statement is never reached.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"
*/
RETURN [{ LET $auth = 1; RETURN 0 }, (LET $x = 1)];
RETURN [(LET $x = 1), { LET $auth = 1; RETURN 0 }];
IF $c { LET $auth = 1 } ELSE IF (LET $x = 1) { 2 };
IF (LET $x = 1) { LET $auth = 1 };
{ RETURN 1; FOR $i IN [(LET $x = 1)] { 0 } };
{ RETURN 1; SLEEP (LET $x = 1) };
