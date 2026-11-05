/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7533: an UPDATE committed while a blocking DEFINE INDEX builds was missing from the finished index. A language test runs its statements in order, so the UPDATE here follows the build and this pins the end state only: the index and a scan agree on the changed row. The overlapping shape was checked against a release server (300k rows, UPDATE issued 0.05 s, 0.3 s and 1 s into the build, build still running) and the index held the new value."
issue = 7533

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[{ count: 1999 }]"
*/

DEFINE TABLE t SCHEMALESS;
FOR $i IN 1..=2000 { CREATE type::record("t", $i) SET flag = false RETURN NONE; };
DEFINE INDEX ix ON t FIELDS flag;
UPDATE t:1 SET flag = true RETURN NONE;
SELECT VALUE id FROM t WITH INDEX ix WHERE flag = true;
SELECT VALUE id FROM t WITH NOINDEX WHERE flag = true;
SELECT count() FROM t WITH INDEX ix WHERE flag = false GROUP ALL;
