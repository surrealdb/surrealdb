/**
[env]
imports = ["reproductions/a_computed_reference_field_derives_no_sibling_computed_field_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A record user's read of a COMPUTED reference field, such as `<~doc`, reads the record id and no field, so it derives that field alone: the sibling `boom` throws when derived and nothing below reads it. The control read of `boom` fails."

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
error = true

*/

SELECT count() FROM thing WHERE documents != [] GROUP ALL;
SELECT count() FROM thing WHERE array::len(documents) >= 0 GROUP ALL;
SELECT count() FROM thing WHERE array::len(documents) >= 0 AND tag = 'a' GROUP ALL;

-- The control: a read of the computed field does derive it, so the field is the one that throws.
SELECT boom FROM thing;
