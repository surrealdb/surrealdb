/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record id's key nests through arrays and objects, and the derived storage codec, the SQL formatter and `Value`'s recursive drop all descend into it, so the write guard must walk the key like any other nested value rather than treat a record id as a leaf. The value built here sits at the cap as a plain array (accepted), and it is the record id's key wrapper that carries it past the cap, so it is the storage-write guard walking the key - not the in-memory build - that rejects the create."

[[test.results]]
value = "[{ id: t:[1] }]"

[[test.results]]
value = "[{ id: t:[1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "[t:[1]]"

*/

CREATE type::record('t', [1]);
SELECT * FROM t:[1];
LET $at_cap = array::fold(array::range(0, 255), [], |$acc, $_| [$acc]);
CREATE type::record('t', $at_cap);
SELECT VALUE id FROM t;
