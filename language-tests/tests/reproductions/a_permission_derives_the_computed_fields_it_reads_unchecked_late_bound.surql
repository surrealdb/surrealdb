/**
[env]
imports = ["reproductions/a_permission_derives_the_computed_fields_it_reads_unchecked_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_permission_derives_the_computed_fields_it_reads_unchecked, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

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

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM note ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT id, hidden, locked FROM note:open; };

{ LET $late = fn::late('now'); RETURN UPDATE note:open SET n = 1 RETURN id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE n FROM note:open; };

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM ring ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE members FROM ring:1; };
