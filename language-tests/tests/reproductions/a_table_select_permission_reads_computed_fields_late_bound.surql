/**
[env]
imports = ["reproductions/a_table_select_permission_reads_computed_fields_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_table_select_permission_reads_computed_fields, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[{ d: NONE, p: 'public' }]"

[[test.results]]
value = "[[doc:pub]]"

# A write the table permits returns nothing the caller may not select.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: page:pub }]"

[[test.results]]
value = "[squad:b]"

*/

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc:sec; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc WHERE status = 'secret'; };
{ LET $late = fn::late('now'); RETURN SELECT count() FROM doc GROUP ALL; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc WHERE hidden = true; };

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM page ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM page:pub; };

{ LET $late = fn::late('now'); RETURN SELECT doc.status AS d, page.status AS p FROM holder:1; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE ->links->doc FROM holder:1; };

{ LET $late = fn::late('now'); RETURN UPDATE doc:sec SET n = 1 RETURN id; };
{ LET $late = fn::late('now'); RETURN CREATE doc:new SET status = 'secret' RETURN id; };
{ LET $late = fn::late('now'); RETURN UPDATE page:pub SET n = 1 RETURN id; };

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM squad ORDER BY id; };
