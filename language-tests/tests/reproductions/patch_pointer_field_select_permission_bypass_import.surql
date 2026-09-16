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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: [{ secret: 'shh-a', title: 'A' }, { secret: 'shh-b', title: 'B' }], id: person:1, name: 'x', nested: { secret: 'nested-shh' }, open: 'visible', secret: 'shh' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS
    PERMISSIONS FOR select, create, update FULL;

-- The access user may write the whole record but may not read these two
-- fields. A `PATCH` read pointer must not carry their values anywhere.
DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD nested ON person TYPE object PERMISSIONS FOR select FULL;
DEFINE FIELD nested.secret ON person TYPE string PERMISSIONS FOR select NONE;

-- The same restriction inside an array, which a pointer reaches through a
-- numeric reference token rather than a field name.
DEFINE FIELD details ON person TYPE array<object> PERMISSIONS FOR select FULL;
DEFINE FIELD details[*].secret ON person TYPE string PERMISSIONS FOR select NONE;

CREATE person:1 SET name = 'x', secret = 'shh', open = 'visible', nested = { secret: 'nested-shh' },
    details = [{ title: 'A', secret: 'shh-a' }, { title: 'B', secret: 'shh-b' }];
