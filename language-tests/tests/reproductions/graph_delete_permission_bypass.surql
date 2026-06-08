/**
[env]
imports = ["reproductions/graph_delete_permission_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Deleting a vertex must not bypass an edge table's `PERMISSIONS FOR delete` clause via the internal `purge_edges` cascade. The access user can delete `person` records and can SELECT/CREATE on `knows`, but `knows` denies all deletes. The cascade must therefore leave `knows:edge1` in place."

# Direct DELETE of the denied edge as the access user: the iterator
# silently skips it, so DELETE returns an empty array.
[[test.results]]
value = "[]"

# Sanity check: the edge survived the direct DELETE.
[[test.results]]
value = "[{ id: knows:edge1, in: person:alice, out: person:bob }]"

# DELETE of the endpoint vertex as the same access user. DELETE
# returns an empty array by default; the side-effect we're testing
# is what happens to the connected edge. Before the fix,
# `purge_edges` ran the cascading `DELETE record<->` with permissions
# disabled and silently erased `knows:edge1` here.
[[test.results]]
value = "[]"

# After the vertex delete, the edge must still exist because the
# access user is not allowed to delete it.
[[test.results]]
value = "[{ id: knows:edge1, in: person:alice, out: person:bob }]"

*/

DELETE knows:edge1;
SELECT * FROM knows:edge1;
DELETE person:alice;
SELECT * FROM knows:edge1;
