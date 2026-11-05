/**
[test]
run = false
reason = "Setup only: a NOT NULL domain over json, whose base kind admits NULL so its NOT NULL is an assertion, and a table with a column of it, read by domain_and_composite_catalog_types"
*/

DEFINE TYPE d_json_nn AS any ASSERT $value != NONE AND $value != NULL SQL TYPE POSTGRESQL 'json';
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD body ON doc TYPE d_json_nn;
