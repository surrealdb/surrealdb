/**
[test]
reason = "Setup for a_count_derives_no_computed_field_it_does_not_read: two tables a record user reads row by row, one whose COMPUTED field cannot be derived and one whose COMPUTED field can."

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

-- A field defined at an array index puts the table's rows through the row-by-row read a record user
-- gets, which is the read under test. The permission reads no field of the record.
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select WHERE $access = 'user';
DEFINE FIELD geo ON person TYPE option<array>;
DEFINE FIELD geo[0] ON person TYPE number;

-- Deriving this field fails, so a read that derives it is a read that fails.
DEFINE FIELD boom ON person COMPUTED { THROW "the computed field was derived" };

CREATE person:1 SET age = 10 RETURN NONE;
CREATE person:2 SET age = 20 RETURN NONE;
CREATE person:3 SET age = 30 RETURN NONE;

-- The same table shape with a computed field that derives, for the aggregates that do read one.
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE $access = 'user';
DEFINE FIELD geo ON item TYPE option<array>;
DEFINE FIELD geo[0] ON item TYPE number;
DEFINE FIELD doubled ON item COMPUTED age * 2;

CREATE item:1 SET age = 10 RETURN NONE;
CREATE item:2 SET age = 20 RETURN NONE;
CREATE item:3 SET age = 30 RETURN NONE;
