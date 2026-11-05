/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1587
reason = """
A write rolled back to a save point takes the value shapes it recorded with
it. An INSERT whose array collides with a unique entry another record holds is
rolled back to its save point and runs its ON DUPLICATE KEY UPDATE on that
record instead; when the update stores the array the attempt wrote, it records
the array shape again, so the index plans its column as one that may hold an
array and the indexed reads answer what their WITH NOINDEX twins answer."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, v: 1 }]"

[[test.results]]
value = "[{ id: u:2, v: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: u:1, v: [1] }, { id: u:2, v: 5 }]"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "[u:1, u:2]"

[[test.results]]
value = "[u:1, u:2]"

*/

DEFINE TABLE u SCHEMALESS;
DEFINE INDEX u_v ON u FIELDS v UNIQUE;
CREATE u:1 SET v = 1;
CREATE u:2 SET v = 5;
-- The create attempt records the array and finds its element 1 held by u:1:
-- it is rolled back, and the update of u:1 that replaces it stores the same
-- array.
INSERT INTO u { v: [1] } ON DUPLICATE KEY UPDATE v = [1] RETURN NONE;
SELECT id, v FROM u ORDER BY id;
SELECT VALUE id FROM u ORDER BY v DESC LIMIT 1;
SELECT VALUE id FROM u WITH NOINDEX ORDER BY v DESC LIMIT 1;
SELECT VALUE id FROM u WHERE v > 3 ORDER BY id;
SELECT VALUE id FROM u WITH NOINDEX WHERE v > 3 ORDER BY id;
