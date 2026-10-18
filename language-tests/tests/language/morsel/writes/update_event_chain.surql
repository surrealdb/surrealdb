/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "an UPDATE of a table whose events write the next record of the same table and records in another table leaves the records the serial run leaves, whatever width its compute half runs at"

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
value = "[NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 8, 9]"

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[891, 892, 893, 894, 895, 896, 897, 898, 899, 900]"

[[test.results]]
value = "[{ total: 404568 }]"

*/

DEFINE TABLE t;
DEFINE TABLE log;
DEFINE EVENT bump ON t WHEN $event = 'UPDATE' AND $after.n < 4 AND !$after.bumped THEN {
	UPDATE type::record('t', $after.id.id() + 1) SET bumped = true;
};
DEFINE EVENT note ON t WHEN $event = 'UPDATE' THEN {
	UPSERT type::record('log', $after.id.id()) SET n = $after.n;
};
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i };
UPDATE t SET n = n + 1 WHERE n < 8 RETURN NONE;
SELECT VALUE bumped FROM t:0..10;
SELECT VALUE n FROM t:0..10;
SELECT count() FROM log GROUP ALL;
UPDATE t SET n = n + 1 WHERE n >= 890 RETURN NONE;
SELECT VALUE n FROM log WHERE n > 889 ORDER BY n;
SELECT math::sum(n) AS total FROM t GROUP ALL;
