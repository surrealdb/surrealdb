/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A RETURN sets the value of the context it stands in and ends it. A context is a statement of the
request or of a transaction, a function or closure body, or an evaluation a statement makes per row
(a WHERE, a projection). A block, an IF or FOR in value or statement position, a parenthesised
statement, an operand, an array element, a function argument, an IF condition and a LET's value
are no context: a RETURN in any of them ends the nearest context around it with its value, so
`RETURN { IF true { RETURN 5 }; 6 } + 1` answers 5 and a LET whose value returns binds nothing.
In a WHERE, a RETURN decides the row it is evaluated for.
"""

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "11"

[[test.results]]
value = "-1"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "'if'"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "7"

[[test.results]]
value = "[ret_a:3]"

[[test.results]]
value = "5"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

LET $d = IF true { RETURN 1 } ELSE { 2 };
RETURN $d;
LET $f = FOR $i IN [1, 2] { RETURN $i };
RETURN $f;
LET $v = (RETURN 8);
RETURN $v;
RETURN { IF true { RETURN 5 }; 6 } + 1;
RETURN [1, { RETURN 11 }, 3];
RETURN math::abs({ RETURN -1; 5 });
RETURN IF ({ RETURN true; false }) { 'a' } ELSE { 'b' };
SELECT VALUE $this FROM [1, 2] WHERE { RETURN false; true };
DEFINE FUNCTION fn::inner() { { RETURN 1 }; RETURN 2 };
RETURN fn::inner();
LET $c = |$a| { { RETURN 1 }; 2 };
RETURN $c(0);
IF true { RETURN 'if' };
IF false { 1 } ELSE IF true { RETURN 2 };
FOR $i IN [1, 2] { RETURN $i };
{ RETURN 7 };
CREATE ret_a:3 RETURN VALUE id;
LET $bare = RETURN 5;
RETURN $bare;
SELECT VALUE $this FROM [1, 2] WHERE RETURN false;
