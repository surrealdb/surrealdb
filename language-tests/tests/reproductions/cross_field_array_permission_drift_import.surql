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
value = "[{ id: person:guest }]"

[[test.results]]
value = "[{ id: nested:one, items: [{ n: 0, secret: 's0' }, { n: 1, secret: 's1' }, { n: 2, secret: 's2' }, { n: 3, secret: 's3' }] }]"

[[test.results]]
value = "[{ id: unfiltered:one, items: [{ n: 0, secret: 's0' }, { n: 1, secret: 's1' }, { n: 2, secret: 's2' }, { n: 3, secret: 's3' }] }]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- Two overlapping wildcard permissions on the same array. The parent
-- `items[*]` keeps only the even-`n` elements; the child `items[*].secret`
-- keeps only the value `'s1'`. The parent compacts the array, so a later
-- child decision taken against an original concrete index must still land on
-- the element that index named in the snapshot, not the survivor that moved
-- into it.
DEFINE TABLE nested SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD items ON nested TYPE array<object>;
DEFINE FIELD OVERWRITE items[*] ON nested TYPE object
    PERMISSIONS FOR select WHERE $value.n = 0 OR $value.n = 2;
DEFINE FIELD items[*].n ON nested TYPE int;
DEFINE FIELD items[*].secret ON nested TYPE string
    PERMISSIONS FOR select WHERE $value = 's1';

-- Negative control: the same child permission with a parent that removes
-- nothing. With no compaction the concrete indices stay stable, so only the
-- explicitly allowed `'s1'` survives.
DEFINE TABLE unfiltered SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD items ON unfiltered TYPE array<object>;
DEFINE FIELD OVERWRITE items[*] ON unfiltered TYPE object
    PERMISSIONS FOR select WHERE true;
DEFINE FIELD items[*].n ON unfiltered TYPE int;
DEFINE FIELD items[*].secret ON unfiltered TYPE string
    PERMISSIONS FOR select WHERE $value = 's1';

CREATE person:guest;
CREATE nested:one SET items = [
    { n: 0, secret: 's0' },
    { n: 1, secret: 's1' },
    { n: 2, secret: 's2' },
    { n: 3, secret: 's3' }
];
CREATE unfiltered:one SET items = [
    { n: 0, secret: 's0' },
    { n: 1, secret: 's1' },
    { n: 2, secret: 's2' },
    { n: 3, secret: 's3' }
];
