/**
[env]
# Extra timeout because this test sleeps.
timeout = 10000
context-timeout = 10000

[test]
reason = "Public issue surrealdb/surrealdb#6206: the status of a CONCURRENTLY build alternated between `ready` and `indexing`. The status is read from the build's durable state, which only moves forward: once a read answers `ready`, every later read does too."
issue = 6206

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ building: { compacting: false, initial: 500, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "{ building: { compacting: false, initial: 500, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "{ building: { compacting: false, initial: 500, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "{ building: { compacting: false, initial: 500, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ count: 500 }]"
*/

DEFINE TABLE t SCHEMALESS;
FOR $i IN 1..=500 { CREATE type::record("t", $i) SET v = $i RETURN NONE; };
DEFINE INDEX ix ON t FIELDS v CONCURRENTLY;
SLEEP 1s;
INFO FOR INDEX ix ON t;
INFO FOR INDEX ix ON t;
INFO FOR INDEX ix ON t;
INFO FOR INDEX ix ON t;
SELECT count() FROM t WITH INDEX ix WHERE v >= 1 GROUP ALL;
