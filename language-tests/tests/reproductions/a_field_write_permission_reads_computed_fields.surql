/**
[env]
imports = ["reproductions/a_field_write_permission_reads_computed_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A field-level `PERMISSIONS FOR create, update` predicate that reads a COMPUTED field must see the value that field computes to from the record being written. A COMPUTED field is not stored, so without materialising it the predicate reads `NONE` — a deny-intended clause admits every write, an allow-intended one refuses every write — or, when the statement assigns to the COMPUTED field's name, the value the caller supplied. A denied field keeps its previous value; on a create that is no value at all."

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

UPDATE acct:locked SET balance = 99 RETURN balance;
UPDATE acct:open SET balance = 20 RETURN balance;
CREATE acct:new SET status = 'locked', balance = 5 RETURN balance;

UPDATE vault:locked SET balance = 99 RETURN balance;
UPDATE vault:locked SET balance = 98, unlocked = true RETURN balance;
UPDATE vault:open SET balance = 20 RETURN balance;
CREATE vault:new SET status = 'locked', balance = 5, unlocked = true RETURN balance;

SELECT id, balance FROM acct ORDER BY id;
SELECT id, balance, unlocked FROM vault ORDER BY id;
