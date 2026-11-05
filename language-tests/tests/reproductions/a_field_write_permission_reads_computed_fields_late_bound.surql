/**
[env]
imports = ["reproductions/a_field_write_permission_reads_computed_fields_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_field_write_permission_reads_computed_fields, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

# `acct:locked` is locked: the write to `balance` is reverted.
[[test.results]]
value = "[{ balance: 10 }]"

[[test.results]]
value = "[{ balance: 20 }]"

# Created locked, so the create permission reverts `balance` to nothing.
[[test.results]]
value = "[{ balance: NONE }]"

# `vault:locked` is not unlocked, and assigning `unlocked` does not make it so:
# the value a COMPUTED field is judged by is the one it computes to.
[[test.results]]
value = "[{ balance: 10 }]"

[[test.results]]
value = "[{ balance: 10 }]"

[[test.results]]
value = "[{ balance: 20 }]"

[[test.results]]
value = "[{ balance: NONE }]"

# The stored state: every denied write was reverted, and the assignment to a
# COMPUTED field's name was not stored.
[[test.results]]
value = "[{ balance: 10, id: acct:locked }, { balance: NONE, id: acct:new }, { balance: 20, id: acct:open }]"

[[test.results]]
value = "[{ balance: 10, id: vault:locked, unlocked: false }, { balance: NONE, id: vault:new, unlocked: false }, { balance: 20, id: vault:open, unlocked: true }]"

*/

{ LET $late = fn::late('now'); RETURN UPDATE acct:locked SET balance = 99 RETURN balance; };
{ LET $late = fn::late('now'); RETURN UPDATE acct:open SET balance = 20 RETURN balance; };
{ LET $late = fn::late('now'); RETURN CREATE acct:new SET status = 'locked', balance = 5 RETURN balance; };

{ LET $late = fn::late('now'); RETURN UPDATE vault:locked SET balance = 99 RETURN balance; };
{ LET $late = fn::late('now'); RETURN UPDATE vault:locked SET balance = 98, unlocked = true RETURN balance; };
{ LET $late = fn::late('now'); RETURN UPDATE vault:open SET balance = 20 RETURN balance; };
{ LET $late = fn::late('now'); RETURN CREATE vault:new SET status = 'locked', balance = 5, unlocked = true RETURN balance; };

{ LET $late = fn::late('now'); RETURN SELECT id, balance FROM acct ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT id, balance, unlocked FROM vault ORDER BY id; };
