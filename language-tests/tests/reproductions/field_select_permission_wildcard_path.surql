/**
[test]
reason = "Wildcard idioms in DEFINE FIELD … PERMISSIONS (e.g. items[*].secret) must be expanded to concrete paths via Value::each at runtime; each concrete path's permission predicate is evaluated independently."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: cart:1, items: [{ id: 'a', name: 'apple' }, { id: 'b', name: 'banana' }] }]"

[env]
imports = ["reproductions/field_select_permission_wildcard_path_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:guest" }

*/

-- The session is a guest with no special role. The schema defines
-- `items[*].secret` with a permission that requires admin role.
-- Each concrete `items[i].secret` path must be cut individually.

RETURN 'OK';

-- Each item's `secret` is removed; `id` and `name` remain.
SELECT * FROM cart:1;
