/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A RETURN written inside the value of a LET or of a RETURN at the top level of a query. In a LET's
value it ends the LET statement, which answers with the returned value and leaves its variable
unbound, so a later `RETURN $x` reads whatever an earlier statement bound to `$x`; the statements
after it still run. In a RETURN's value it is that RETURN's value. A type annotation on the LET is
not checked against the returned value. Inside a transaction it ends the transaction, so the
writes after it are not made. A BREAK in a LET's value ends the enclosing loop. A LET in the
value of a block may not bind a protected variable, and the block's writes are undone.
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
value = "NONE"

[[test.results]]
error = "The table 'ret_k' does not exist"

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
