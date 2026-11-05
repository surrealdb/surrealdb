/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A LET's value is no context, and neither is a block, a value IF or FOR, or a subquery: a RETURN
written inside the value of a LET ends the nearest context around it with its value, and the LET
binds nothing. At the top level that context is the LET's statement, which answers the returned
value; the next statement runs and reads the name unbound. Inside a block the RETURN ends the
context the block stands in, whatever block nests it. A type annotation on the LET is not checked
against a value the LET never binds. Inside a transaction the LET's statement is the context, so
the statements after it run and the transaction commits. A BREAK in a LET's value ends the
enclosing loop. A LET in the value of a block may not bind a protected variable, and the block's
writes are undone.
"""

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
value = "2"

[[test.results]]
value = "'deep'"

[[test.results]]
value = "2"

[[test.results]]
value = "7"

[[test.results]]
value = "[2, NONE]"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "'g'"

[[test.results]]
value = "[2, [ret_g:1]]"

[[test.results]]
value = "'five'"

[[test.results]]
value = "5"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[ret_j:0, ret_j:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ id: ret_k:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[ret_k:1]"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'ret_l' does not exist"

*/

LET $x = { IF true { RETURN 1 }; 2 }; RETURN $x;
LET $x = { IF false { RETURN 1 }; 2 }; RETURN $x;
LET $x = { FOR $i IN 0..5 { IF $i = 3 { RETURN $i } }; 'none' }; RETURN $x;
LET $x = { { { RETURN 'deep' } }; 'shallow' }; RETURN $x;
LET $x = { LET $y = 7; IF true { RETURN $y } }; RETURN [$x, $y];
RETURN { IF true { RETURN 'inner' }; 'outer' };
LET $x = IF true { CREATE ret_g:1; RETURN 'g' } ELSE { 'h' }; RETURN [$x, (SELECT VALUE id FROM ret_g)];
LET $x: int = { RETURN 'five' };
LET $x = { 1 + (RETURN 5) }; RETURN $x;
FOR $i IN 0..5 { LET $x = { IF $i = 2 { BREAK }; $i }; CREATE type::record('ret_j', $x) }; SELECT VALUE id FROM ret_j ORDER BY id;
BEGIN; LET $x = { RETURN 1 }; CREATE ret_k:1; COMMIT; SELECT VALUE id FROM ret_k;
{ CREATE ret_l:1; LET $x = { LET $session = 1; 2 }; RETURN $x }; SELECT VALUE id FROM ret_l;
