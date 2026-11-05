/**
[env]
namespace = true
database = true
imports = ["access/record/computed_field_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A field the caller may not select, projected under its own name, is answered as
`NONE`: no value reaches the caller, but the key is present in the output. An
alias already agrees (the third statement), because a denied field is simply
absent from the reduced record and an absent field projects as `NONE`.

The intended contract is that the key is cut from the output. Cutting it needs
a pass over the projected output keyed on the output name, and that pass also
cuts any output key sharing a guarded field's name, including one an alias put
there; reproducing the cut belongs with the projection operator and a rule
stated over the projected path rather than over the name.
"""

[[test.results]]
value = "[{ hidden: NONE }]"

[[test.results]]
value = "[{ secret: NONE }]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[{ hidden: NONE }]"
*/

-- A denied COMPUTED field, projected under its own name.
SELECT hidden FROM item:1;

-- A denied stored field, which the reduce kernel removes rather than the
-- computed-field pass, answers alike.
SELECT secret FROM item:1;

-- The control: an alias is NONE, because the image it is projected from carries
-- no such field.
SELECT hidden AS alias FROM item:1;

-- A mutation's `RETURN` field list is the same projection, and divides the
-- same way. `access/record/computed_field_select_permission.surql` holds every
-- other shape of that statement.
UPDATE item:1 RETURN hidden;
