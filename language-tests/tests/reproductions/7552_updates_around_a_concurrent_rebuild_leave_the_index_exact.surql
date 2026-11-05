/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7552: auto-commit UPDATEs of an indexed field issued while REBUILD INDEX … CONCURRENTLY runs could leave stale or missing index entries after the build reported ready. A language test runs its statements in order, so the overlap itself is pinned by `normal_write_from_a_snapshot_before_the_final_replay_conflicts` in surrealdb/core/src/kvs/index/tests.rs: a write whose snapshot predates the build's final replay fails with a retryable conflict rather than leave a stale entry; this pins the end state: after updates on either side of a concurrent rebuild, the index and a scan agree on every probed value. An UPDATE issued in the same instant as the rebuild starts can fail with a retryable write conflict against the build's admission state; that is why the updates follow a pause."
issue = 7552

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[{ count: 297 }]"

*/

DEFINE TABLE t SCHEMALESS;
FOR $i IN 1..=300 { CREATE type::record("t", $i) SET x = 'old' RETURN NONE; };
DEFINE INDEX ix ON t FIELDS x CONCURRENTLY;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
UPDATE t:1 SET x = 'new' RETURN NONE;
REBUILD INDEX ix ON t CONCURRENTLY;
SLEEP 1s;
UPDATE t:2 SET x = 'new' RETURN NONE;
UPDATE t:3 SET x = 'third' RETURN NONE;
UPDATE t:3 SET x = 'fourth' RETURN NONE;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON t).building.status != 'ready' { SLEEP 1s };
RETURN (INFO FOR INDEX ix ON t).building.status;
SELECT VALUE id FROM t WITH INDEX ix WHERE x = 'new' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX ix WHERE x = 'third' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX ix WHERE x = 'fourth' ORDER BY id;
SELECT count() FROM t WITH INDEX ix WHERE x = 'old' GROUP ALL;
