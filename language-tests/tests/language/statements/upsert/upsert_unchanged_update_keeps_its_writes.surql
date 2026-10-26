/**
[env]
namespace = true
database = true

[test]
reason = """
A row that runs to its store keeps what it wrote along the way, also when the
store writes no record and `RETURN NONE` withholds the output: suppressing the
output does not change what the statement did. A `DROP` table stores no record,
and a write to one exists for the events and views it triggers, so those
writes stand, for INSERT, UPSERT and CREATE. An update that leaves its record
unchanged keeps what its data clause wrote. Only a row that a gate skips before
it can write is rolled back.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "1"

[[test.results]]
value = "[]"

[[test.results]]
value = "2"

[[test.results]]
value = "[]"

[[test.results]]
value = "3"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "4"

[[test.results]]
value = "[]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ id: r:1, v: 1 }]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE d DROP SCHEMALESS;
DEFINE EVENT log ON d THEN { CREATE audit SET at = $event RETURN NONE };

INSERT INTO d { v: 1 } RETURN NONE;
RETURN count(SELECT * FROM audit);
UPSERT d:1 SET v = 1 RETURN NONE;
RETURN count(SELECT * FROM audit);
CREATE d:2 SET v = 1 RETURN NONE;
RETURN count(SELECT * FROM audit);

CREATE r:1 SET v = 1 RETURN NONE;
UPSERT r:1 SET v = { CREATE audit RETURN NONE; RETURN 1; } RETURN NONE;
RETURN count(SELECT * FROM audit);
INSERT INTO r { id: r:1 } ON DUPLICATE KEY UPDATE v = { CREATE audit RETURN NONE; RETURN 1; } RETURN NONE;
RETURN count(SELECT * FROM audit);
SELECT * FROM r;
