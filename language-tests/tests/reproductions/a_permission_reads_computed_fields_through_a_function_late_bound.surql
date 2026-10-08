/**
[env]
imports = ["reproductions/a_permission_reads_computed_fields_through_a_function_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_permission_reads_computed_fields_through_a_function, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[note:pub]"

[[test.results]]
value = "[]"

# The write gates refuse a locked record, and a refused record is filtered out
# of the result.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

# A field whose write is refused keeps its previous value.
[[test.results]]
value = "[{ balance: 10 }]"

[[test.results]]
value = "[{ balance: 20 }]"

[[test.results]]
value = "[{ id: acct:locked, n: NONE }, { id: acct:open, n: 1 }]"

*/

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc:sec; };
{ LET $late = fn::late('now'); RETURN SELECT count() FROM doc GROUP ALL; };

{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM note ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM note:sec; };

{ LET $late = fn::late('now'); RETURN UPDATE acct:locked SET n = 1 RETURN n; };
{ LET $late = fn::late('now'); RETURN DELETE acct:locked RETURN BEFORE; };
{ LET $late = fn::late('now'); RETURN CREATE acct:new SET status = 'locked' RETURN id; };
{ LET $late = fn::late('now'); RETURN UPDATE acct:open SET n = 1 RETURN n; };

{ LET $late = fn::late('now'); RETURN UPDATE vault:locked SET balance = 99 RETURN balance; };
{ LET $late = fn::late('now'); RETURN UPDATE vault:open SET balance = 20 RETURN balance; };

{ LET $late = fn::late('now'); RETURN SELECT id, n FROM acct ORDER BY id; };
