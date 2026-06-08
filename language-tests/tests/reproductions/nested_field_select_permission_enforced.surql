/**
[test]
reason = "Issue #83: nested-path SELECT permissions (DEFINE FIELD outer.a … PERMISSIONS FOR SELECT WHERE …) must be enforced at the streaming runtime. Pre-fix, the streaming filter iterated only top-level keys and silently leaked the restricted nested field."

issue = 83

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: foo:1, outer: { b: 'public' } }]"

[[test.results]]
value = "[{ id: foo:1, outer: { b: 'public' } }]"

[[test.results]]
value = "[{ outer: { b: 'public' } }]"

[env]
imports = ["reproductions/nested_field_select_permission_enforced_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

*/

-- The session is a record-level access for `person:bob`, who is *not* in
-- the admin set. The permission predicate
-- `WHERE $auth.role = 'admin'` must filter `outer.a` from the result,
-- leaving `outer.b` visible.

-- Setup confirmation: the access definition succeeded.
RETURN 'OK';

-- SELECT *: the restricted nested field is removed; `outer.b` survives.
SELECT * FROM foo;

-- Specific record path: same outcome.
SELECT * FROM foo:1;

-- Project just the parent: `outer.a` is gone, `outer.b` is present.
SELECT outer FROM foo;
