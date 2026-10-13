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
value = "[{ blocked: true, id: person:1, name: 'x' }]"

[[test.results]]
value = "[{ blocked: true, id: plain:1, name: 'x' }]"

[[test.results]]
value = "[{ blocked: true, id: doomed:1, name: 'x' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- `blocked` is COMPUTED, so it is not stored. A predicate naming it must
-- still see `true`, otherwise the deny is a no-op.
DEFINE TABLE person SCHEMALESS
    PERMISSIONS FOR select FULL, FOR update WHERE blocked != true;
DEFINE FIELD blocked ON person COMPUTED true;

-- Control table: the identical predicate over a plain stored field.
DEFINE TABLE plain SCHEMALESS
    PERMISSIONS FOR select FULL, FOR update WHERE blocked != true;

-- The delete gate shares the same evaluation path.
DEFINE TABLE doomed SCHEMALESS
    PERMISSIONS FOR select FULL, FOR delete WHERE blocked != true;
DEFINE FIELD blocked ON doomed COMPUTED true;

CREATE person:1 SET name = 'x';
CREATE plain:1 SET name = 'x', blocked = true;
CREATE doomed:1 SET name = 'x';
