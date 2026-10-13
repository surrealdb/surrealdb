/**
[env]
imports = ["reproductions/a_write_permission_computes_fields_in_dependency_order_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table-level write `PERMISSIONS` predicate that reads a COMPUTED field must see the value that field computes to, and a COMPUTED field that reads another one must be derived after it. The write gate materialises the fields its predicate reads, so they have to be computed in dependency order, the order the streaming read path uses: in name order `can_edit` is derived from `is_locked = NONE` before `is_locked` exists, reads `!NONE = true`, and admits a write to a locked record."

# `is_locked` is true, so `can_edit` is false and the update is rejected. A
# rejected record is filtered out of the result.
[[test.results]]
value = "[]"

# The delete gate reads the same fields. `RETURN BEFORE` would return the
# record had the delete been admitted.
[[test.results]]
value = "[]"

# The create gate is judged against the record about to be created.
[[test.results]]
value = "[]"

# An unlocked record is still writable.
[[test.results]]
value = "[{ n: 1 }]"

# Nothing was created, modified or removed on a locked record, and the read
# path derives the same values the gate judged.
[[test.results]]
value = "[{ can_edit: false, id: t:locked, is_locked: true, n: NONE }, { can_edit: true, id: t:open, is_locked: false, n: 1 }]"

*/

UPDATE t:locked SET n = 1;
DELETE t:locked RETURN BEFORE;
CREATE t:new SET status = 'locked';
UPDATE t:open SET n = 1 RETURN n;
SELECT id, is_locked, can_edit, n FROM t ORDER BY id;
