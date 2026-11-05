/**
[env]
imports = ["reproductions/a_permission_derives_a_computed_field_under_its_auth_limit_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_permission_derives_a_computed_field_under_its_auth_limit, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

*/

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc:sec; };
{ LET $late = fn::late('now'); RETURN SELECT count() FROM doc GROUP ALL; };
