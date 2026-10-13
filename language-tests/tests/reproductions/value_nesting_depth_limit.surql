/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A stored value's nesting depth must be bounded. The public-value conversion every query result passes through, the SQL formatter used by export and INFO, the derived storage codec, and `Value`'s own recursive drop all walk a value with no depth guard, and a stack overflow in any of them aborts the whole process under the release profile's `panic = 'abort'`. The parser's caps are per-parse, so a read-modify-write loop adding one level per request never approaches them; the depth of the value itself has to be rejected at the point it is written."

# A value nested well within the cap stores and reads back normally.
[[test.results]]
value = "[{ id: shallow:1, v: [[[[[1]]]]] }]"

[[test.results]]
value = "[{ id: shallow:1, v: [[[[[1]]]]] }]"

# Growing a value one level per statement is the accumulation vector. The
# statements below each add a level, and the one that crosses the cap is
# rejected rather than stored.
[[test.results]]
value = "[{ id: deep:1, v: [] }]"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

# The rejected write left the record as it was, so the guard fails closed
# rather than storing a truncated value.
[[test.results]]
value = "[{ id: deep:1, v: [] }]"

*/

CREATE shallow:1 SET v = [[[[[1]]]]];
SELECT * FROM shallow:1;
CREATE deep:1 SET v = [];
UPDATE deep:1 SET v = array::fold(array::range(0, 300), [], |$acc, $_| [$acc]);
SELECT * FROM deep:1;
