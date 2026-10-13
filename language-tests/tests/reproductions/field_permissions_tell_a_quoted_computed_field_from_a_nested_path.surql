/**
[env]
imports = ["reproductions/field_permissions_tell_a_quoted_computed_field_from_a_nested_path_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A field permission on a nested stored field is applied before the computed fields are re-derived, even when a top-level COMPUTED field's quoted name spells the same path (`is.locked`). The read path tells a computed field's permission from a stored one's by whether the computed fields are known by its name; known by the same name, the nested field is taken for the computed one, its value is cut only after re-derivation, and the computed field reading it carries the denied value out."

[[test.results]]
value = """[{ id: p:1, is: {  }, "is.locked": NONE }]"""

[[test.results]]
value = """[{ "is.locked": NONE }]"""

[[test.results]]
value = "[]"
*/

SELECT * FROM p;
SELECT `is.locked` FROM p;
SELECT VALUE id FROM p WHERE `is.locked` = true;
