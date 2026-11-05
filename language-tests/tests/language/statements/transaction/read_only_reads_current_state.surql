/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "BEGIN READ ONLY, with and without MAX STALENESS and STRICT, reads the current state on a datastore with no stale tier, and the words READ, MAX and STALENESS stay usable as names."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: read:max, staleness: 2 }]"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"
*/
DEFINE TABLE item SCHEMALESS;
CREATE item:one SET value = 1;
DEFINE TABLE read SCHEMALESS;
CREATE read:max SET staleness = 2;

BEGIN READ ONLY;
SELECT VALUE value FROM item;
COMMIT;

BEGIN TRANSACTION READ ONLY MAX STALENESS 5s;
SELECT VALUE value FROM item;
COMMIT;

BEGIN READ ONLY MAX STALENESS 0s STRICT;
SELECT VALUE value FROM item;
COMMIT;

SELECT VALUE staleness FROM read;
