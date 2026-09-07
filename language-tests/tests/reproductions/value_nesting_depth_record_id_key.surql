/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record id's key nests through arrays and objects, and the derived storage codec, the SQL formatter and `Value`'s recursive drop all descend into it. The depth guard treated a record id as a leaf, so a compound id built past the cap at runtime — `type::record` over a deeply nested array — was stored and then aborted the process on the next read. The key has to be walked by the same guard as a field value."

# A shallow compound id stores and reads back normally.
[[test.results]]
value = "[{ id: t:[1] }]"

[[test.results]]
value = "[{ id: t:[1] }]"

# A compound id nested past the cap is rejected at the write, not stored.
[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

# Only the shallow record exists; the rejected write stored nothing.
[[test.results]]
value = "[t:[1]]"

*/

CREATE type::record('t', [1]);
SELECT * FROM t:[1];
CREATE type::record('t', array::fold(array::range(0, 300), [], |$acc, $_| [$acc]));
SELECT VALUE id FROM t;
