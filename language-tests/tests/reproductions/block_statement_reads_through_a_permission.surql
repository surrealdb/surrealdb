/**
[test]
reason = "A statement a block or RETURN runs reads through a table permission that reads another table as a statement on its own does: the permission's own read is unchecked. Binding the block's statement again must keep the predicate's reads unchecked."

[[test.results]]
value = "[{ id: item:0 }, { id: item:1 }, { id: item:2 }, { id: item:3 }, { id: item:4 }]"

[[test.results]]
value = "[{ id: item:0 }, { id: item:1 }, { id: item:2 }, { id: item:3 }, { id: item:4 }]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ id: item:0 }, { id: item:1 }, { id: item:2 }, { id: item:3 }, { id: item:4 }]"

[[test.results]]
value = "[{ id: item:0 }, { id: item:1 }, { id: item:2 }, { id: item:3 }, { id: item:4 }]"

[[test.results]]
value = "[]"

[env]
imports = ["reproductions/count_permission_parity_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

*/

RETURN SELECT id FROM item;
RETURN (SELECT id FROM item);
RETURN array::len((SELECT id FROM item));
{ LET $n = SELECT id FROM item; RETURN $n };
IF true { SELECT id FROM item };
RETURN SELECT id FROM secret;
