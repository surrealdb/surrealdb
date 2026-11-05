/**
[test]
reason = "Setup for reference_custom_delete_permission_bypass. Run as owner by the file that imports it."

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
value = "[{ id: person:alice }]"

[[test.results]]
value = "[{ id: person:bob }]"

[[test.results]]
value = "[{ id: doc:d, ref: person:alice }]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- The access user may fully manage `person` records, so deleting
-- `person:alice` is a delete it is entitled to perform.
DEFINE TABLE person SCHEMALESS
    PERMISSIONS FOR select, create, delete FULL;

-- `vault` is a table the access user may read but never write.
DEFINE TABLE vault SCHEMALESS
    PERMISSIONS
        FOR select FULL,
        FOR create, update, delete NONE;

-- `doc` references a `person`. Its `ON DELETE THEN` repair is a user-supplied
-- expression that writes to `vault`, which the deleting caller may not do.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select, create FULL;

DEFINE FIELD ref ON TABLE doc TYPE record<person>
    REFERENCE ON DELETE THEN (UPSERT vault:leaked SET breached = true);

CREATE person:alice;
CREATE person:bob;
CREATE doc:d SET ref = person:alice;
