/**
[test]
reason = "Field-permission predicates referencing $value must see the field's picked value, not an unbound NONE. Pre-fix, the streaming runtime never bound $value, so any predicate using it evaluated against NONE and either always-failed or always-passed depending on coercion."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: doc:1, owner: 'bob', visible_for: ['bob', 'carol'] }]"

[[test.results]]
value = "[{ id: doc:2, owner: 'alice' }]"

[[test.results]]
value = "[{ id: doc:3, owner: 'carol' }]"

[env]
imports = ["reproductions/field_select_permission_value_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

*/

-- Bob is the auth subject. The permission on `visible_for` is
-- `WHERE $auth.id IN $value` — it expects `$value` to be bound to the
-- field's array value at the concrete path. Without the binding,
-- `$auth.id IN NONE` evaluates to false and the field would always be
-- cut, even when bob is in the list.

-- Setup confirmation.
RETURN 'OK';

-- doc:1's `visible_for` includes bob → field survives.
SELECT * FROM doc:1;

-- doc:2's `visible_for` excludes bob → field is cut. The remaining
-- `owner` field is unaffected.
SELECT * FROM doc:2;

-- doc:3 has no `visible_for` field. The non-wildcard idiom expansion
-- yields zero concrete paths, so the predicate never runs and the row
-- passes through unmodified — same shape as the legacy `pluck.rs` path.
SELECT * FROM doc:3;
