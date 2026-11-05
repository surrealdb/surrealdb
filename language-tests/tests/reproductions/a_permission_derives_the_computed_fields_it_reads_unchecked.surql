/**
[env]
imports = ["reproductions/a_permission_derives_the_computed_fields_it_reads_unchecked_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A COMPUTED field a table permission reads is derived as the permission itself is evaluated: with reads unchecked, so the field reads what its body would read written in the predicate in its place. The caller receives the field as the caller would read it. Derived as the caller, a body following a record the caller may not select derives from `NONE`, which satisfies a clause written to deny, and a body reading its own table re-enters that table's permission."

# `note:secret` is hidden: its `meta` is secret, whether or not the caller may
# read `meta`.
[[test.results]]
value = "[note:open]"

# What the caller reads of the fields is what the caller may read.
[[test.results]]
value = "[{ hidden: NONE, id: note:open, locked: NONE }]"

# `note:open` is locked through a record the caller may not select.
[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[ring:1, ring:2]"

[[test.results]]
value = "[2]"

*/

SELECT VALUE id FROM note ORDER BY id;
SELECT id, hidden, locked FROM note:open;

UPDATE note:open SET n = 1 RETURN id;
SELECT VALUE n FROM note:open;

SELECT VALUE id FROM ring ORDER BY id;
SELECT VALUE members FROM ring:1;
