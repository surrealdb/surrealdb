/**
[env]
imports = ["reproductions/7416_auth_field_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:u" }
timeout = 5000

[test]
reason = "A field permission over $auth is row-independent, so SELECT id does not evaluate the table's COMPUTED fields (an erroring one proves it is never read)"
issue = 7416

[[test.results]]
value = "[{ id: item:1 }, { id: item:2 }]"

[[test.results]]
value = "[{ admin_note: 'n', id: item:1, name: 'a' }, { admin_note: NONE, id: item:2, name: 'b' }]"
*/

SELECT id FROM item ORDER BY id;
SELECT id, name, admin_note FROM item ORDER BY id;
