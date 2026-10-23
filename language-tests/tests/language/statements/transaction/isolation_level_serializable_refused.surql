/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["surrealkv"]

[test]
reason = "A backend that offers only SNAPSHOT refuses SERIALIZABLE, naming the level it offers, rather than quietly giving a weaker one."
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The isolation level SERIALIZABLE is not available on this datastore; it offers SNAPSHOT"

[[test.results]]
error = "Cannot COMMIT: The isolation level SERIALIZABLE is not available on this datastore; it offers SNAPSHOT"

[[test.results]]
value = "[]"

*/
DEFINE TABLE item SCHEMALESS;

BEGIN ISOLATION LEVEL SERIALIZABLE;
CREATE item:one SET value = 1;
COMMIT;

SELECT VALUE value FROM item;
