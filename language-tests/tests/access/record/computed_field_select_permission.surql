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
A COMPUTED field's own `PERMISSIONS FOR select` has to hold everywhere a
statement can read the field, not only where the record is projected whole.
The reduce kernel cannot enforce it — it runs before any computed field
exists — so every view that materialises them has to filter them afterwards.
Without that a caller who may update the record reads a denied value through
a data clause, a `WHERE` condition, or a renamed `RETURN` field, and can copy
it into a field they are allowed to select. A field the caller *is* allowed to
read stays readable in all the same places.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[{ counter: 1, id: item:1, visible: 'VISIBLE' }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: NONE }]"

[[test.results]]
value = "[{ leak: 'VISIBLE' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ leak: 'matched' }]"

[[test.results]]
value = "[{  }]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[{ alias: 'VISIBLE' }]"

[[test.results]]
value = "[{ alias: NONE }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ alias: 'VISIBLE' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:1 }]"

[[test.results]]
value = "[{ counter: 4, id: item:1, leak: 'matched', visible: 'VISIBLE' }]"
*/

RETURN $auth;

-- The record user's view of the record: every denied field is absent, stored
-- and computed alike, and the allowed computed field is present.
SELECT * FROM item:1;

-- A data clause cannot read a denied computed field, so it cannot copy one
-- into a field the caller may select. Same for a denied stored field.
UPDATE item:1 SET leak = hidden RETURN leak;
UPDATE item:1 SET leak = conditional RETURN leak;
UPDATE item:1 SET leak = derived RETURN leak;
UPDATE item:1 SET leak = secret RETURN leak;
-- Every data-clause form and every updating statement reads that same image.
UPDATE item:1 MERGE { leak: hidden } RETURN leak;
UPSERT item:1 SET leak = hidden RETURN leak;
-- The allowed computed field still reaches the data clause.
UPDATE item:1 SET leak = visible RETURN leak;

-- A `WHERE` condition reads the same image as the data clause, so a denied
-- computed field cannot select rows to write either.
UPDATE item:1 SET leak = 'matched' WHERE hidden = 'COMPUTED_SECRET' RETURN leak;
UPDATE item:1 SET leak = 'matched' WHERE derived = 2 RETURN leak;
-- The allowed computed field still matches, and the write happens.
UPDATE item:1 SET leak = 'matched' WHERE visible = 'VISIBLE' RETURN leak;

-- A `RETURN` field list is projected from that same image, and an alias does
-- not smuggle a denied field past the permission pass on the output. The two
-- mechanisms answer differently in shape: a field projected under its own name
-- is cut from the output, while an alias resolves to `NONE` because the image
-- it was projected from no longer carries the field.
UPDATE item:1 RETURN hidden;
UPDATE item:1 RETURN hidden AS alias;
UPDATE item:1 RETURN VALUE hidden;
UPDATE item:1 SET counter = 4 RETURN $after.derived AS alias;
UPDATE item:1 RETURN visible AS alias;

-- The read side answers the same, whichever way the field is projected.
SELECT hidden AS alias FROM item:1;
SELECT VALUE hidden FROM item:1;
SELECT visible AS alias FROM item:1;
SELECT id FROM item WHERE hidden = 'COMPUTED_SECRET';
SELECT id FROM item WHERE visible = 'VISIBLE';

-- Nothing above wrote a denied value into the record.
SELECT * FROM item:1;
