/**
[env]
namespace = true
database = true

[test]
reason = """
A row's record id is generated before its create attempt's save point, so an
`ON DUPLICATE KEY UPDATE` conflict, which rolls the attempt back and updates
the conflicting record instead, keeps the id's draw from a sequence. The next
row draws the next value and creates a new record, rather than drawing the
same value again and updating the same record twice. The sequence is defined
in the same transaction, the case a save point rollback restores.
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
value = "[t:0, t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:0, v: 1 }, { id: t:1, v: 2 }]"
*/

BEGIN;
DEFINE SEQUENCE seq;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD id ON t DEFAULT sequence::nextval('seq');
CREATE t:0 SET v = 0 RETURN NONE;
INSERT INTO t [{ v: 1 }, { v: 2 }] ON DUPLICATE KEY UPDATE v = $input.v RETURN VALUE id;
COMMIT;
SELECT id, v FROM t ORDER BY id;
