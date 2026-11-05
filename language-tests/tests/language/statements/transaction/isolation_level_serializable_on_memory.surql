/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["mem"]

[test]
reason = "The memory engine offers SERIALIZABLE, for read-write and read-only transactions alike."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

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
*/
DEFINE TABLE item SCHEMALESS;

BEGIN ISOLATION LEVEL SERIALIZABLE;
CREATE item:one SET value = 1;
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY;
SELECT VALUE value FROM item;
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY MAX STALENESS 0s;
SELECT VALUE value FROM item;
COMMIT;
