/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ISOLATION LEVEL SNAPSHOT is the default, so naming it changes nothing; a SERIALIZABLE read cannot be combined with a MAX STALENESS above zero; the words stay usable as names."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: level:snapshot, isolation: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:two, value: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "ISOLATION LEVEL SERIALIZABLE cannot be combined with a MAX STALENESS above zero"

[[test.results]]
error = "Cannot COMMIT: ISOLATION LEVEL SERIALIZABLE cannot be combined with a MAX STALENESS above zero"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[3]"
*/
DEFINE TABLE item SCHEMALESS;
CREATE item:one SET value = 1;
DEFINE TABLE level SCHEMALESS;
CREATE level:snapshot SET isolation = 3;

BEGIN ISOLATION LEVEL SNAPSHOT;
CREATE item:two SET value = 2;
COMMIT;

BEGIN ISOLATION LEVEL SNAPSHOT READ ONLY;
SELECT VALUE value FROM item ORDER BY value;
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY MAX STALENESS 5s;
SELECT VALUE value FROM item;
COMMIT;

SELECT VALUE value FROM item ORDER BY value;
SELECT VALUE isolation FROM level;
