/**
[env]
imports = ["reproductions/a_write_permission_computes_fields_in_dependency_order_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_write_permission_computes_fields_in_dependency_order, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

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

{ LET $late = fn::late('now'); RETURN UPDATE t:locked SET n = 1; };
{ LET $late = fn::late('now'); RETURN DELETE t:locked RETURN BEFORE; };
{ LET $late = fn::late('now'); RETURN CREATE t:new SET status = 'locked'; };
{ LET $late = fn::late('now'); RETURN UPDATE t:open SET n = 1 RETURN n; };
{ LET $late = fn::late('now'); RETURN SELECT id, is_locked, can_edit, n FROM t ORDER BY id; };
