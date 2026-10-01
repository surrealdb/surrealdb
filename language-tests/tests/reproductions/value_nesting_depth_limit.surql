/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A stored value's nesting depth must be bounded at the write. The public-value conversion every query result passes through, the SQL formatter used by export and INFO, the derived storage codec, and `Value`'s own recursive drop all walk a value with no depth guard, and a stack overflow in any of them aborts the whole process under the release profile's `panic = 'abort'`. The parser's caps are per-parse, so a read-modify-write loop that adds one level per request never approaches them; the depth of the stored value itself is what has to be rejected. This exercises the storage-write guard directly: the value grows in the record (not in a fold accumulator), so the write guard is the check that rejects it."

[[test.results]]
value = "[{ id: shallow:1, v: [[[[[1]]]]] }]"

[[test.results]]
value = "[{ id: shallow:1, v: [[[[[1]]]]] }]"

[[test.results]]
value = "[{ id: deep:1, v: [] }]"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "[deep:1]"

*/

CREATE shallow:1 SET v = [[[[[1]]]]];
SELECT * FROM shallow:1;
CREATE deep:1 SET v = [];
FOR $_ IN array::range(0, 300) { UPDATE deep:1 SET v = [v]; };
SELECT VALUE id FROM deep:1;
