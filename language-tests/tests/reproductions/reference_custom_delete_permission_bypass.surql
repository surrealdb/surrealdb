/**
[env]
imports = ["reproductions/reference_custom_delete_permission_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A `REFERENCE ON DELETE THEN` repair is a user-supplied expression, so it must run under the deleting caller's own permissions, not the referential-integrity bypass that the system-internal cascade mechanics need. `vault` denies the access user every write, and `person:bob` may delete `person:alice`; deleting it fires `doc:d`'s `ON DELETE THEN`, which writes to `vault`. That write must be refused exactly as the caller's own direct write to `vault` is, so `vault` stays empty. Companion to `reference_cascade_edge_delete_permission_bypass.surql`, which covers the edge cascade routed through a CASCADE repair."

# A direct write to `vault` as the access user is denied, so it stores nothing.
[[test.results]]
value = "[]"

# `vault` is empty: the access user has written nothing to it directly.
[[test.results]]
value = "[]"

# Deleting `person:alice` is a delete the access user is entitled to perform;
# it fires `doc:d`'s `ON DELETE THEN` repair.
[[test.results]]
value = "[]"

# The `ON DELETE THEN` repair is a user-supplied expression and runs under the
# caller's permissions, so its write to `vault` is refused just like the direct
# one above: `vault` is still empty.
[[test.results]]
value = "[]"
*/

UPSERT vault:direct SET breached = true;
SELECT * FROM vault;
DELETE person:alice;
SELECT * FROM vault;
