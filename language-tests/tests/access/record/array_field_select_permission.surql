/**
[env]
namespace = true
database = true
imports = ["access/record/array_field_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
Applying a field's `PERMISSIONS FOR select` to a value means cutting the paths
that field's name widens to, and a name widens across an array element by
element — so it addresses a key *within* each element, never the element itself.
Widening it to the elements' own paths instead cut the whole element, which cost
the caller every sibling key it was allowed to read, and erased an array-shaped
result outright when no element even carried the field.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[{ id: t:1, items: [{ shown: 1 }, { shown: 2 }, { shown: 3 }] }]"

[[test.results]]
value = "[{ items: [{ shown: 1 }, { shown: 2 }, { shown: 3 }] }]"

[[test.results]]
value = "[[{ shown: 1 }, { shown: 2 }, { shown: 3 }]]"

[[test.results]]
value = "[[{ shown: 9 }]]"

[[test.results]]
value = "[{ id: t:1, items: [{ shown: 4 }, { shown: 5 }] }]"
*/

RETURN $auth;

-- The denied key is cut from each element; every element and every sibling key
-- it was allowed to read survives.
SELECT * FROM t:1;
SELECT items FROM t:1;

-- An array-shaped result is not a record image, and a denied field that no
-- element carries must leave it untouched rather than empty it.
SELECT VALUE items FROM t:1;
UPDATE t:1 SET items = [{ shown: 9, denied: 'WRITTEN_SECRET' }] RETURN VALUE items;

-- The same holds for a whole-record write output.
UPDATE t:1 SET items = [{ shown: 4, denied: 'x' }, { shown: 5 }] RETURN AFTER;
