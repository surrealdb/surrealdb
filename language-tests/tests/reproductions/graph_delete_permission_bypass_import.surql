/**
[test]

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
value = "[{ id: knows:edge1, in: person:alice, out: person:bob }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS
    PERMISSIONS FOR select, create, delete FULL;

-- `knows` lets the access user read and create edges but explicitly
-- forbids edge deletion. The bug under test allowed a vertex DELETE
-- to cascade through `purge_edges` with permissions disabled, erasing
-- the edge despite this clause.
DEFINE TABLE knows TYPE RELATION IN person OUT person SCHEMALESS
    PERMISSIONS
        FOR select, create FULL,
        FOR delete NONE;

CREATE person:alice;
CREATE person:bob;
RELATE person:alice->knows->person:bob SET id = knows:edge1;
