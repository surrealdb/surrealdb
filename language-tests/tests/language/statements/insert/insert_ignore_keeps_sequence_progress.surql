/**
[env]
namespace = true
database = true

[test]
reason = """
An `INSERT IGNORE` row whose `id` DEFAULT draws an id that already names a
record is skipped, and the sequence still moves past that id: the next row in
the batch draws the next value and is created. A sequence defined in the same
transaction is restored by the rollback of the skipped row's create attempt,
so the draw is replayed after it; without that, every following row would draw
the same id and be skipped in turn. A sequence committed earlier is never
restored, and behaves the same.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[p:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p:0, p:1, p:2]"

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
value = "[p2:2]"

[[test.results]]
value = "[p2:0, p2:1, p2:2]"
*/

-- A sequence defined in the same transaction
BEGIN;
DEFINE SEQUENCE sq;
DEFINE TABLE p SCHEMALESS;
DEFINE FIELD id ON p DEFAULT sequence::nextval('sq');
INSERT INTO p {} RETURN NONE;
INSERT INTO p { id: p:1 } RETURN NONE;
INSERT IGNORE INTO p [{}, {}] RETURN VALUE id;
COMMIT;
SELECT VALUE id FROM p ORDER BY id;

-- A sequence committed earlier
DEFINE SEQUENCE sq2;
DEFINE TABLE p2 SCHEMALESS;
DEFINE FIELD id ON p2 DEFAULT sequence::nextval('sq2');
INSERT INTO p2 {} RETURN NONE;
INSERT INTO p2 { id: p2:1 } RETURN NONE;
INSERT IGNORE INTO p2 [{}, {}] RETURN VALUE id;
SELECT VALUE id FROM p2 ORDER BY id;
