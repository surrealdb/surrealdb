/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A RETURN written inside the value of a LET is caught by the value scope it stands in: a block, a
value IF or FOR, a subquery. The LET binds the caught value and answers NONE, so a later `RETURN $x`
reads that value. A RETURN that stands in no value scope ends its top-level statement with its value.
A statement-level IF or FOR inside the scope passes its RETURN to the scope, and a block nested in
the scope as a statement is a value scope of its own, so
`{ { { RETURN 'deep' } }; 'shallow' }` binds 'shallow'. A type annotation on the LET is checked against the caught value. Inside
a transaction a caught RETURN ends only its scope, so the statements after the LET run and the
transaction commits. A BREAK in a LET's value ends the enclosing loop. A LET in the value of a block
may not bind a protected variable, and the block's writes are undone. In an
expression, `{ 1 + (RETURN 5) }` adds the subquery's value, so `$x` is 6: a statement in scalar
position that is a value scope takes its own RETURN and resumes after itself.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'shallow'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[7, NONE]"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['g', [ret_g:1]]"

[[test.results]]
error = "Tried to set `$x`, but couldn't coerce value: Expected `int` but found `'five'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[ret_j:0, ret_j:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
