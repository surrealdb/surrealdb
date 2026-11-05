/**
[env]
imports = ["reproductions/a_permission_reads_computed_fields_through_a_function_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A permission predicate that reads a COMPUTED field through a function evaluated against the record — a user-defined function whose body reads the field, or `type::field` — must see the value that field computes to. The predicate names no field, so judged by its own text it reads nothing that needs deriving, and the function then reads the field as stored: as `NONE`, which satisfies a clause written to deny."

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

SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM doc:sec;
SELECT count() FROM doc GROUP ALL;

SELECT VALUE id FROM note ORDER BY id;
SELECT VALUE id FROM note:sec;

UPDATE acct:locked SET n = 1 RETURN n;
DELETE acct:locked RETURN BEFORE;
CREATE acct:new SET status = 'locked' RETURN id;
UPDATE acct:open SET n = 1 RETURN n;

UPDATE vault:locked SET balance = 99 RETURN balance;
UPDATE vault:open SET balance = 20 RETURN balance;

SELECT id, n FROM acct ORDER BY id;
