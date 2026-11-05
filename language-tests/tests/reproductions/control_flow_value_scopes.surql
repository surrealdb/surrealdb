/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A statement in the value of an IF arm, closure body or function body that is a parenthesised FOR,
IF or subquery stands in an expression and yields its value, NONE for a FOR, as a subquery does. A
RETURN written in parentheses as the value of a LET is no context: it ends the context the LET
stands in, so `RETURN { LET $x = (RETURN 'a'); RETURN 'after' }` answers 'a'. A top-level LET of a
parenthesised RETURN answers that value as the statement's result, and the next statement runs.
"""

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'none'"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[1]]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'after'"

*/

RETURN [1].map(|$i| IF true THEN (FOR $j IN [5] { }) END);
RETURN [1].map(|$i| IF false THEN 0 ELSE (FOR $j IN [5] { }) END);
RETURN [1].map(|$i| IF true THEN 1 + (FOR $j IN [5] { }) END);
RETURN [1].map(|$i| IF true THEN (IF true THEN (FOR $j IN [5] { }) END) END);
RETURN [1].map(|$i| IF true THEN (SELECT * FROM [1]) ELSE 0 END);
RETURN [1].map(|$i| IF true THEN (THROW 'boom') ELSE 1 END);
LET $f = |$i| IF true THEN (FOR $j IN [5] { }) END; RETURN $f(1);
DEFINE FUNCTION fn::cfvs_for($i: int) { RETURN IF true THEN (FOR $j IN [5] { }) END; };
RETURN fn::cfvs_for(1);
RETURN { LET $x = (RETURN 'a'); RETURN 'after' };
RETURN { LET $x = (RETURN 'a'); RETURN $x };
RETURN { LET $x = (RETURN 'a'); LET $y = (RETURN 'b'); RETURN [$x, $y] };
LET $x = (RETURN 'a'); RETURN 'after';
