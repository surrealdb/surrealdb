/**
[env]
namespace = true
database = true
imports = ["access/record/computed_field_conditional_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A COMPUTED field's `PERMISSIONS FOR select` predicate is answered by the record,
not by the permission-reduced view of it. The reduced view is missing the fields
the caller may not select, so a predicate naming one of them reads it as `NONE`
there — `WHERE secret = NONE` holds on the view for a record whose `secret` is
set, and passes a field the clause was written to deny. The reduce kernel
evaluates a stored field's clause against the record for this reason, and the
computed-field filter has to agree with it.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[{ counter: 1, gated_by_visible: 'GATED_OK', id: item:1 }]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[{ alias: 'GATED_OK' }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: 'GATED_OK' }]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/counter', value: 2 }]]"

[[test.results]]
value = "[{ leak: 'GATED_OK' }]"
*/

RETURN $auth;

-- The field gated on a denied stored field is absent, because its predicate is
-- answered by the record where that field is set. The one gated on a field the
-- caller may select is present.
SELECT * FROM item:1;

-- Every way of reading it agrees, on either side of a write.
SELECT gated_by_denied AS alias FROM item:1;
SELECT gated_by_visible AS alias FROM item:1;
UPDATE item:1 SET leak = gated_by_denied RETURN leak;
UPDATE item:1 SET leak = gated_by_visible RETURN leak;
UPDATE item:1 RETURN gated_by_denied AS alias;
UPDATE item:1 SET counter = 2 RETURN DIFF;

-- Nothing above copied the denied value into the readable field.
SELECT leak FROM item:1;
