/**
[test]
reason = "Setup for field_permissions_tell_a_quoted_computed_field_from_a_nested_path.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ id: p:1, is: { locked: true }, "is.locked": true }]"""
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- The nested field `is.locked` is denied; the top-level field `is.locked`,
-- whose quoted name spells the same path, is computed from it.
DEFINE TABLE p SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD is ON p TYPE object;
DEFINE FIELD is.locked ON p TYPE bool PERMISSIONS FOR select NONE;
DEFINE FIELD `is.locked` ON p COMPUTED is.locked;
CREATE p:1 SET is = { locked: true };
