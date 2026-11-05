/**
[env]
imports = ["language/write/computed_field_names_that_need_escaping_in_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = """
A permission that reads a `COMPUTED` field is judged against the value the field
derives, because the field is not stored and reads `NONE` off the record as it
is. A name that has to be backticked when written has to be derived under the name
itself for that to hold: a clause written to deny (`!= true`) would otherwise admit
every row, and one written to allow (`= true`) would admit none. This covers the
table's `select` permission, a field's `select` permission, and a field's write
permission.
"""

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[{ id: acct:locked, note: NONE }, { id: acct:open, note: 'n1' }]"

[[test.results]]
value = "[{ balance: 20 }]"

[[test.results]]
value = "[{ balance: 10 }]"
*/

-- Deny-intended: the secret document is withheld.
SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM doc:sec;
SELECT VALUE id FROM doc WHERE status = 'secret';
SELECT count() FROM doc GROUP ALL;

-- Allow-intended, through a chain of computed fields.
SELECT VALUE id FROM page ORDER BY id;
SELECT VALUE id FROM page:pub;

-- A field's select permission cuts the field on the locked record alone.
SELECT id, note FROM acct ORDER BY id;

-- A field's write permission reverts the change on the locked record alone.
UPDATE acct:open SET balance = 20 RETURN balance;
UPDATE acct:locked SET balance = 20 RETURN balance;
