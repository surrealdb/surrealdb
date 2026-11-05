/**
[test]
reason = "Setup for 3.00.00_record_format_mixed.surql: the old binary writes `revision` records into a schemaless and a SCHEMAFULL table, the latter with an index over a field the body rewrites."
run = false
*/
DEFINE TABLE doc SCHEMALESS;
CREATE doc:1 SET n = 1, s = 'one', f = 1.5f, d = <decimal> 2.5, dt = d"2012-01-01T08:00:00Z", u = u"00000000-0000-0000-0000-000000000001", tags = ['a', 'b'], obj = { x: 1, y: { z: 'deep' } }, link = person:a, nothing = NULL;
CREATE doc:2 SET n = 2, s = 'two';
CREATE doc:3 SET n = 3, s = 'three', obj = { x: 3 };
DEFINE TABLE typed SCHEMAFULL;
DEFINE FIELD n ON typed TYPE int;
DEFINE FIELD s ON typed TYPE string;
DEFINE FIELD opt ON typed TYPE option<string>;
DEFINE FIELD tags ON typed TYPE array<string>;
DEFINE INDEX typed_n ON typed FIELDS n;
CREATE typed:1 SET n = 1, s = 'a string longer than twelve bytes', tags = ['x'];
CREATE typed:2 SET n = 2, s = 'b', opt = 'present', tags = [];
