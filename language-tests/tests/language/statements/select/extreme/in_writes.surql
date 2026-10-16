/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of an indexed `int` field read by a statement that writes the same table, in the
	value a write assigns, in the condition of a write, and between the writes of one statement: a
	row a write changes is read as changed by the evaluations after it.
"""

[[test.results]]
value = "NONE"

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
value = "[]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 11 }, { id: t:3, n: 12 }, { id: t:4, n: 13 }]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 11 }, { id: t:3, n: 12 }, { id: t:4, n: 13 }]"

[[test.results]]
value = "[{ id: t:1, n: 9 }, { id: t:2, n: 8 }, { id: t:3, n: 7 }, { id: t:4, n: 6 }]"

[[test.results]]
value = "[{ id: t:1, n: 9 }, { id: t:2, n: 8 }, { id: t:3, n: 7 }, { id: t:4, n: 6 }]"

[[test.results]]
value = "[{ id: t:5, n: 9 }]"

[[test.results]]
value = "[{ id: t:6, n: 6 }]"

[[test.results]]
value = "[{ id: t:7, n: 109 }, { id: t:8, n: 9 }]"

[[test.results]]
value = "[{ id: t:1, n: 9 }, { id: t:2, n: 8 }, { id: t:3, n: 7 }, { id: t:4, n: 6 }, { id: t:5, n: 9 }, { id: t:6, n: 6 }, { id: t:7, n: 109 }, { id: t:8, n: 9 }]"

[[test.results]]
value = "[{ id: t:7, n: 109 }, { id: t:8, n: 9 }]"

[[test.results]]
value = "[{ id: t:1, n: 9 }, { id: t:2, n: 8 }, { id: t:3, n: 7 }, { id: t:4, n: 6 }, { id: t:5, n: 9 }, { id: t:6, n: 6 }]"

[[test.results]]
value = "[{ id: t:2, n: 9 }, { id: t:3, n: 8 }, { id: t:4, n: 7 }, { id: t:6, n: 7 }]"

[[test.results]]
value = "[{ id: t:20, n: 7 }]"

[[test.results]]
value = "[{ hi: 9, lo: 7 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 1000 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: -1000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 9 }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE INDEX n ON t FIELDS n;
CREATE t:1 SET n = 3 RETURN NONE;
CREATE t:2 SET n = 9 RETURN NONE;
CREATE t:3 SET n = 9 RETURN NONE;
CREATE t:4 SET n = -4 RETURN NONE;
UPDATE t SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 1 RETURN id, n;
SELECT id, n FROM t ORDER BY id;
UPDATE t SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] - 1 RETURN id, n;
SELECT id, n FROM t ORDER BY id;
CREATE t:5 SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN id, n;
CREATE t:6 SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] RETURN id, n;
INSERT INTO t (id, n) VALUES (t:7, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 100), (t:8, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0]);
SELECT id, n FROM t ORDER BY id;
DELETE t WHERE n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN BEFORE;
SELECT id, n FROM t ORDER BY id;
UPDATE t SET n = n + 1 WHERE n < (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN id, n;
UPSERT t:20 SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] RETURN id, n;
SELECT math::max(n) AS hi, math::min(n) AS lo FROM t GROUP ALL;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
BEGIN;
CREATE t:30 SET n = 1000 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
UPDATE t:30 SET n = -1000 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
COMMIT;
SELECT math::max(n) AS m FROM t GROUP ALL;
