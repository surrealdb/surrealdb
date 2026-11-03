/**
[test]
run = false
reason = "Setup only: a type over a kind and a type over an object defined in SurrealQL, in the form INFO FOR DB and an export print them, read by user_types_defined_in_surrealql"
*/

DEFINE TYPE posint AS i32 CONSTRAINT positive ASSERT $value > 0 SQL TYPE POSTGRESQL 'integer';
DEFINE TYPE address AS { street: none | null | string, zip: none | null | i32 } SQL TYPE POSTGRESQL '(street text, zip int)';
