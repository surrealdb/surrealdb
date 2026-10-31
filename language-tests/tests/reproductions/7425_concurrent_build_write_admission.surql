/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7425: writes that change an indexed field while a CONCURRENTLY build runs are neither lost nor stalled indefinitely; the build reaches ready and the index agrees with a scan. Such a write waits in admission only while the build is Closing, which drains what writers committed during the build's last replay batch; a build in the same process releases the waiting writes when it publishes (`a_write_held_by_closing_is_released_when_the_build_publishes`), and a build stranded in Closing by a dead owner is adopted by the next resume pass even while other builds run (`a_running_build_defers_stalled_scans_but_not_a_stalled_closing`). Sustained writers are pinned by `a_build_comes_online_while_writers_keep_queueing`; all three are in surrealdb/core/src/kvs/index/tests.rs."
issue = 7425

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

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 500, pending: 0, status: 'ready', updated: 1 }"

[[test.results]]
value = "[item:1, item:2]"
*/

DEFINE TABLE item SCHEMALESS;
FOR $i IN 1..=500 { CREATE type::record("item", $i) SET scope = 'a', v = 0 RETURN NONE; };
DEFINE INDEX ix ON item FIELDS scope, v CONCURRENTLY;
UPDATE item:1 SET v = 7 RETURN NONE;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON item).building.status != 'ready' { SLEEP 1s };
UPDATE item:2 SET v = 8 RETURN NONE;
INFO FOR INDEX ix ON item;
SELECT VALUE id FROM item WITH INDEX ix WHERE scope = 'a' AND v > 0 ORDER BY id;
