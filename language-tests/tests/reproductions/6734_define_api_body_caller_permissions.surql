/**
[env]
namespace = "test"
database = "test"
imports = ["api/security/invoker_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
issue = 6734
reason = "A DEFINE API body that selects from a table the caller has permissions on runs with those permissions lifted: a record user who is shown one row and no `secret` field by a direct query gets every row and every secret from the API. That is the documented definer-rights behaviour and stays the default, so the plain API still returns everything. SECURITY INVOKER is the opt-in that makes the body run as the caller, and returns exactly what the direct query does."

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"
*/

SELECT * FROM category ORDER BY id;
(api::invoke("/default/read")).body;
(api::invoke("/invoker/read")).body;
