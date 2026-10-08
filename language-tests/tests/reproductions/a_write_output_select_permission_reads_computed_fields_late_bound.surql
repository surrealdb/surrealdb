/**
[env]
imports = ["reproductions/a_write_output_select_permission_reads_computed_fields_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_write_output_select_permission_reads_computed_fields, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

# The pre-image is hidden; the delete itself is permitted.
[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:pub]"

# The post-image decides: an update that makes a record public shows it, one
# that makes it secret does not.
[[test.results]]
value = "[{ id: doc:sec2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: page:pub }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:sec2]"

[[test.results]]
value = "[page:pub]"

*/

{ LET $late = fn::late('now'); RETURN DELETE doc:sec RETURN BEFORE; };
{ LET $late = fn::late('now'); RETURN DELETE doc:pub RETURN VALUE $before.id; };

{ LET $late = fn::late('now'); RETURN UPDATE doc:sec2 SET status = 'public' RETURN id; };
{ LET $late = fn::late('now'); RETURN UPDATE doc:pub2 SET status = 'secret' RETURN id; };

{ LET $late = fn::late('now'); RETURN UPDATE page:pub SET n = 1 RETURN id; };
{ LET $late = fn::late('now'); RETURN UPDATE page:sec SET n = 1 RETURN id; };

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM page ORDER BY id; };
