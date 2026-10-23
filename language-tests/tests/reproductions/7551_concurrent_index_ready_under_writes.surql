/**
[env]
timeout = 5000
context-timeout = 5000

[test]
issue = 7551
reason = "A CONCURRENTLY build reaches ready, and its index matches the records, when writes to the indexed field interleave with the build. Sustained concurrent writers cannot be driven from one language-test connection; that case is pinned by `a_build_comes_online_while_writers_keep_queueing` in surrealdb/core/src/kvs/index/tests.rs, which commits a write before every read of the catch-up replay."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:3]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE |t:1..=500| SET x = 0 RETURN NONE;
DEFINE INDEX ix ON t FIELDS x CONCURRENTLY;
UPDATE t:1, t:2 SET x = 1 RETURN NONE;
UPDATE t:3 SET x = 2 RETURN NONE;
SLEEP 1s;
(INFO FOR INDEX ix ON t).building.status;
SELECT VALUE id FROM t WITH INDEX ix WHERE x = 1 ORDER BY id;
SELECT VALUE id FROM t WITH INDEX ix WHERE x = 2;
