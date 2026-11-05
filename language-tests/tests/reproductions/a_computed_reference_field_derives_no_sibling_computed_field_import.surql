/**
[test]
reason = "Setup for a_computed_reference_field_derives_no_sibling_computed_field."

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
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

*/
DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE thing SCHEMALESS PERMISSIONS FOR select WHERE $access = 'user';
-- A field defined at an array index puts the rows through the row-by-row read a record user gets.
DEFINE FIELD geo ON thing TYPE option<array>;
DEFINE FIELD geo[0] ON thing TYPE number;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON doc TYPE record<thing> REFERENCE;
DEFINE FIELD documents ON thing COMPUTED <~doc;
-- Deriving this field fails, so a read that derives it is a read that fails.
DEFINE FIELD boom ON thing COMPUTED { THROW "the computed field was derived" };

CREATE thing:1 SET tag = 'a' RETURN NONE;
CREATE thing:2 SET tag = 'b' RETURN NONE;
CREATE doc:1 SET owner = thing:1 RETURN NONE;
