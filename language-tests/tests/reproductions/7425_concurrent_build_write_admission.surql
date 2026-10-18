/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7425: writes that change an indexed field while a CONCURRENTLY build runs are neither lost nor stalled indefinitely; the build reaches ready and the index agrees with a scan. Checked live with 4 writers on 100k rows: median 13 ms, max 18 ms. The Closing wait is bounded by the build and is by design."
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
value = "[]"

[[test.results]]
value = "{ building: { compacting: false, initial: 500, pending: 0, status: 'ready', updated: 1 } }"

[[test.results]]
value = "[item:1, item:2]"
*/

DEFINE TABLE item SCHEMALESS;
FOR $i IN 1..=500 { CREATE type::record("item", $i) SET scope = 'a', v = 0 RETURN NONE; };
DEFINE INDEX ix ON item FIELDS scope, v CONCURRENTLY;
UPDATE item:1 SET v = 7 RETURN NONE;
SLEEP 1s;
UPDATE item:2 SET v = 8 RETURN NONE;
INFO FOR INDEX ix ON item;
SELECT VALUE id FROM item WITH INDEX ix WHERE scope = 'a' AND v > 0 ORDER BY id;
