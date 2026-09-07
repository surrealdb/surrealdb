/**
[test]

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
value = "[{ id: person:carol }]"

[[test.results]]
value = "[{ id: holder:h, ref: person:alice }]"

[[test.results]]
value = "[{ id: guard:g, in: holder:h, out: person:carol }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS
    PERMISSIONS FOR select, create, delete FULL;

-- `guard` lets the access user read and create edges but explicitly
-- forbids edge deletion.
DEFINE TABLE guard TYPE RELATION IN holder OUT person SCHEMALESS
    PERMISSIONS
        FOR select, create FULL,
        FOR delete NONE;

-- `holder` is both a `guard` endpoint and the referencing record of a
-- `REFERENCE ON DELETE CASCADE` field, which is the combination that routes
-- the edge cascade through a permissions-disabled frame.
DEFINE TABLE holder SCHEMALESS
    PERMISSIONS FOR select, create FULL;

DEFINE FIELD ref ON TABLE holder TYPE record<person> REFERENCE ON DELETE CASCADE;

CREATE person:alice;
CREATE person:bob;
CREATE person:carol;
CREATE holder:h SET ref = person:alice;
RELATE holder:h->guard->person:carol SET id = guard:g;
