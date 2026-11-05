/**
[env]
imports = ["reproductions/7465_permission_clause_probe_forms_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A permission clause is definer code and reads other tables unchecked, whatever the spelling of the read: a record-id dereference, a subquery and `record::exists` all admit through a lookup table the caller cannot read, in a select clause and in a create clause alike."
issue = 7465

[[test.results]]
value = "[item_a:one]"

[[test.results]]
value = "[item_b:one]"

[[test.results]]
value = "[item_c:one]"

[[test.results]]
value = "[{ id: item_c:two }]"

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM item_a;
SELECT VALUE id FROM item_b;
SELECT VALUE id FROM item_c;
CREATE item_c:two;
SELECT * FROM lookup;
