/**
[test]
reason = "Setup for the reproductions of a COMPUTED field or a table permission whose body reads its own table, read as a record user."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- A computed field that reads its own table, on a table the caller may read.
DEFINE TABLE ring SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD members ON ring COMPUTED count(SELECT VALUE id FROM ring);
CREATE ring:1 RETURN NONE;
CREATE ring:2 RETURN NONE;

-- A table permission that calls a function reading the permission's own table.
DEFINE FUNCTION fn::seen() { RETURN count(SELECT VALUE id FROM gated) };
DEFINE TABLE gated SCHEMALESS PERMISSIONS FOR select WHERE fn::seen() > 0;
CREATE gated:1 RETURN NONE;
CREATE gated:2 RETURN NONE;
