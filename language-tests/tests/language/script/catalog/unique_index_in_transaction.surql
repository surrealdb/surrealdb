/**
[test]
requires = ["scripting"]
reason = """
A unique index an embedded script defines inside a transaction refuses,
where it is written, a value a committed row holds, which fails the
transaction, and nothing it wrote is kept.
"""

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"
*/
CREATE t:1 SET a = 1;
BEGIN;
(function() { return surrealdb.query("DEFINE INDEX u ON t FIELDS a UNIQUE") });
CREATE t:0 SET a = 2;
CREATE t:2 SET a = 1;
COMMIT;
SELECT * FROM t;
