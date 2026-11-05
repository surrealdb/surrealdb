/**
[env]
imports = ["reproductions/reference_cascade_edge_delete_permission_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "An edge table's `PERMISSIONS FOR delete` clause must hold when the endpoint delete is routed through a `REFERENCE ON DELETE CASCADE`. That branch disables permissions so the referencing record's own gate cannot block referential integrity, and `Options::perms` is frame-wide, so the bypass reached the graph-edge cascade too. `guard` denies all deletes, so `guard:g` must survive both the direct delete and the cascade-routed one. Companion to `graph_delete_permission_bypass.surql`, which covers only the direct vertex-delete path."

# Direct DELETE of the denied edge as the access user: the iterator
# silently skips it, so DELETE returns an empty array.
[[test.results]]
value = "[]"

# Sanity check: the edge survived the direct DELETE.
[[test.results]]
value = "[{ id: guard:g, in: holder:h, out: person:carol }]"

# DELETE the referenced vertex as the same access user. This cascades to
# `holder:h` (the referencing record), whose own delete runs with
# permissions disabled for referential integrity. Deleting `holder:h`
# purges its graph edges, and that cascade must run under the caller's
# permissions rather than inheriting the integrity bypass.
[[test.results]]
value = "[]"

# The CASCADE removed the referencing record, as its strategy requires.
[[test.results]]
value = "[]"

# The protected edge must still exist: the access user may not delete it.
[[test.results]]
value = "[{ id: guard:g, in: holder:h, out: person:carol }]"

# The far endpoint is untouched, so the cascade did not simply fail wholesale.
[[test.results]]
value = "[{ id: person:carol }]"

*/

DELETE guard:g;
SELECT * FROM guard:g;
DELETE person:alice;
SELECT * FROM holder:h;
SELECT * FROM guard:g;
SELECT * FROM person:carol;
