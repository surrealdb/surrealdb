/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A range on the leading column of a compound index, plain or UNIQUE, must include or exclude the boundary row exactly as the same predicate does without the index"
issue = 7532

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: false, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: false, id: t:2 }]"

[[test.results]]
value = "[{ a: 3, b: false, id: t:3 }]"

[[test.results]]
value = "[{ a: 4, b: false, id: t:4 }]"

[[test.results]]
value = "[{ a: 5, b: false, id: t:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[1, 2, 3]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1, b = false;
CREATE t:2 SET a = 2, b = false;
CREATE t:3 SET a = 3, b = false;
CREATE t:4 SET a = 4, b = false;
CREATE t:5 SET a = 5, b = false;
DEFINE INDEX ab ON t FIELDS a, b;
SELECT VALUE a FROM t WHERE b = false AND a > 3 ORDER BY a;
SELECT VALUE a FROM t WHERE b = false AND a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE a > 1 AND a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE b = false AND a > 1 AND a <= 3 ORDER BY a;
SELECT count() FROM t WHERE a <= 3 GROUP ALL;
REMOVE INDEX ab ON t;
DEFINE INDEX ab ON t FIELDS a, b UNIQUE;
SELECT VALUE a FROM t WHERE b = false AND a > 3 ORDER BY a;
SELECT VALUE a FROM t WHERE b = false AND a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE a > 1 AND a <= 3 ORDER BY a;
SELECT VALUE a FROM t WHERE b = false AND a > 1 AND a <= 3 ORDER BY a;
SELECT count() FROM t WHERE a <= 3 GROUP ALL;
UPDATE t SET touched = true WHERE a <= 3 RETURN VALUE a;
