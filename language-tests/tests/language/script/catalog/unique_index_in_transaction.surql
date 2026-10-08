/**
[test]
reason = "A unique index an embedded script defines inside a transaction over rows that violate it fails the transaction, and nothing it wrote is kept."

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

# The commit fails with the storage engine's own write-conflict error, whose
# text differs between engines.
[[test.results]]
error = true

[[test.results]]
value = "[{ a: 1, id: t:1 }]"
*/
CREATE t:1 SET a = 1;
BEGIN;
CREATE t:0 SET a = 2;
(function() { return surrealdb.query("DEFINE INDEX u ON t FIELDS a UNIQUE") });
CREATE t:2 SET a = 1;
COMMIT;
SELECT * FROM t;
