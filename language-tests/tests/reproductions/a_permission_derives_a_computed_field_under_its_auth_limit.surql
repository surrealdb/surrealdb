/**
[env]
imports = ["reproductions/a_permission_derives_a_computed_field_under_its_auth_limit_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A COMPUTED field a table permission reads is derived for it under the field's AUTH LIMIT, a call of a user-defined function or a statement in its body included: the permission sees the value the field computes to, so a deny-intended clause hides the record."

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

*/

SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM doc:sec;
SELECT count() FROM doc GROUP ALL;
