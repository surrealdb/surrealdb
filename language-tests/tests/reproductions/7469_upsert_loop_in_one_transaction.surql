/**
[env]
namespace = true
database = true

[test]
reason = "Many UPSERTs in one transaction, each taking and releasing a savepoint, leave the final value of every record, a unique index and a COUNT index that agree with the rows, and a later UPSERT that fails rolls back only its own write. This pins the results only: whether a release pops the savepoint is not observable from SurrealQL, and is guarded per backend by the kvs-test release_outermost_then_rollback_errors."
issue = 7469

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "50"

[[test.results]]
value = "8725"

[[test.results]]
value = "[t:7]"

[[test.results]]
value = "[t:49]"

[[test.results]]
value = "[{ count: 50 }]"

[[test.results]]
error = "Database index `ic` already contains 7, with record `t:7`"

[[test.results]]
value = "50"

[[test.results]]
value = "[t:7]"
*/

DEFINE TABLE t;
DEFINE INDEX ia ON t FIELDS a;
DEFINE INDEX ic ON t FIELDS c UNIQUE;
DEFINE INDEX cnt ON t COUNT;

BEGIN;
FOR $i IN 0..200 {
	UPSERT type::record('t', $i % 50) CONTENT { a: $i, c: $i % 50 };
};
COMMIT;

count(SELECT * FROM t);
math::sum(SELECT VALUE a FROM t);
SELECT VALUE id FROM t WHERE c = 7;
SELECT VALUE id FROM t WHERE a = 199;
SELECT count() FROM t GROUP ALL;

UPSERT t:extra CONTENT { a: 1000, c: 7 };
count(SELECT * FROM t);
SELECT VALUE id FROM t WHERE c = 7;
