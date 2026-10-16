/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of an `int` field under a `UNIQUE` index, the one index the plan answers an extreme
	from, read by a statement that writes the same table: in the value a write assigns, in the
	condition of a write, between the writes of one statement, in a loop and in a transaction over
	rows it has not committed. A row a write changes is read as changed by the evaluations after it.
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
value = "[{ id: t:1, n: 11 }, { id: t:2, n: 12 }, { id: t:3, n: 13 }, { id: t:4, n: 14 }]"

[[test.results]]
value = "[{ id: t:1, n: 11 }, { id: t:2, n: 12 }, { id: t:3, n: 13 }, { id: t:4, n: 14 }]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 9 }, { id: t:3, n: 8 }, { id: t:4, n: 7 }]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 9 }, { id: t:3, n: 8 }, { id: t:4, n: 7 }]"

[[test.results]]
value = "[{ id: t:5, n: 11 }]"

[[test.results]]
value = "[{ id: t:6, n: 6 }]"

[[test.results]]
error = "Database index `n` already contains 11, with record `t:5`"

[[test.results]]
value = "[{ id: t:7, n: 111 }, { id: t:8, n: 12 }]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 9 }, { id: t:3, n: 8 }, { id: t:4, n: 7 }, { id: t:5, n: 11 }, { id: t:6, n: 6 }, { id: t:7, n: 111 }, { id: t:8, n: 12 }]"

[[test.results]]
value = "[{ id: t:7, n: 111 }, { id: t:8, n: 12 }]"

[[test.results]]
value = "[{ id: t:1, n: 10 }, { id: t:2, n: 9 }, { id: t:3, n: 8 }, { id: t:4, n: 7 }, { id: t:5, n: 11 }, { id: t:6, n: 6 }]"

[[test.results]]
value = "[{ id: t:1, n: 1010 }, { id: t:2, n: 1009 }, { id: t:3, n: 1008 }, { id: t:4, n: 1007 }, { id: t:5, n: 1011 }, { id: t:6, n: 1006 }]"

[[test.results]]
value = "[{ id: t:20, n: 1005 }]"

[[test.results]]
value = "[{ hi: 1011, lo: 1005 }]"

[[test.results]]
value = "[{ m: 1011 }]"

[[test.results]]
value = "[{ m: 1005 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 1000000 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 1011 }]"

[[test.results]]
value = "[{ m: -1000000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 1011 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 1011 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1014, 1013, 1012, 1011]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[-1000003, -1000002, -1000001, -1000000]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE INDEX n ON t FIELDS n UNIQUE;
CREATE t:1 SET n = 3 RETURN NONE;
CREATE t:2 SET n = 9 RETURN NONE;
CREATE t:3 SET n = 10 RETURN NONE;
CREATE t:4 SET n = -4 RETURN NONE;
UPDATE t SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 1 RETURN id, n;
SELECT id, n FROM t ORDER BY id;
UPDATE t SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] - 1 RETURN id, n;
SELECT id, n FROM t ORDER BY id;
CREATE t:5 SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 1 RETURN id, n;
CREATE t:6 SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] - 1 RETURN id, n;
CREATE t:9 SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN id, n;
INSERT INTO t (id, n) VALUES (t:7, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 100), (t:8, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 1);
SELECT id, n FROM t ORDER BY id;
DELETE t WHERE n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN BEFORE;
SELECT id, n FROM t ORDER BY id;
UPDATE t SET n = n + 1000 WHERE n < (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] RETURN id, n;
UPSERT t:20 SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] - 1 RETURN id, n;
SELECT math::max(n) AS hi, math::min(n) AS lo FROM t GROUP ALL;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
BEGIN;
CREATE t:30 SET n = 1000000 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
UPDATE t:30 SET n = -1000000 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
COMMIT;
SELECT math::max(n) AS m FROM t GROUP ALL;
BEGIN;
CREATE t:31 SET n = 2000000 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
CANCEL;
SELECT math::max(n) AS m FROM t GROUP ALL;
FOR $i IN [1, 2, 3] { CREATE t SET n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] + 1; };
SELECT VALUE n FROM t ORDER BY n DESC LIMIT 4;
FOR $i IN [1, 2, 3] { CREATE t SET n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] - 1; };
SELECT VALUE n FROM t ORDER BY n LIMIT 4;
