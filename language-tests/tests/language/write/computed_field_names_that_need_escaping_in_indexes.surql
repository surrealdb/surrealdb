/**
[env]
namespace = true
database = true

[test]
reason = """
An index over a stored field whose name has to be backticked when written is
maintained under that name through every write and answers a read through the
index, and a `COMPUTED` field of the same table that reads it is filtered on by its
own name. A `COMPUTED` field cannot itself be indexed, so the index is on the
stored field and the computed one is read through it. This holds with or without
the escaping handled in the write path, so it guards the neighbours of the
computed-field derivations rather than catching the escaping bug itself.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ id: j:1, "stored x": 'a', "the x": 'a' }]"""

[[test.results]]
value = """[{ id: j:2, "stored x": 'b', "the x": 'b' }]"""

[[test.results]]
value = """[{ id: j:3, "stored x": 'a', "the x": 'a' }]"""

[[test.results]]
value = "[j:1, j:3]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'by_stored', table: 'j' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = """[{ id: j:3, "stored x": 'c', "the x": 'c' }]"""

[[test.results]]
value = "[j:1]"

[[test.results]]
value = "[j:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE j SCHEMALESS;
DEFINE FIELD `stored x` ON j TYPE option<string>;
DEFINE INDEX by_stored ON j FIELDS `stored x`;
DEFINE FIELD `the x` ON j COMPUTED `stored x`;

CREATE j:1 SET `stored x` = 'a';
CREATE j:2 SET `stored x` = 'b';
CREATE j:3 SET `stored x` = 'a';

-- The index answers the read, and the plan says it is the one used.
SELECT VALUE id FROM j WHERE `stored x` = 'a' ORDER BY id;
SELECT VALUE id FROM j WHERE `stored x` = 'a' ORDER BY id EXPLAIN;

-- It follows an update and a delete.
UPDATE j:3 SET `stored x` = 'c';
SELECT VALUE id FROM j WHERE `stored x` = 'a' ORDER BY id;
SELECT VALUE id FROM j WHERE `the x` = 'c' ORDER BY id;
DELETE j:1;
SELECT VALUE id FROM j WHERE `stored x` = 'a' ORDER BY id;
