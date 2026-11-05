/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An `UPSERT` under a condition no record satisfies still creates the record its target names
when none exists (the guaranteed record is created by the order that holds no `WHERE`),
and skips a record that exists. A table target with no record creates one. Every
stage of its orders is bound whatever the condition.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:2, x: 1 }]"

[[test.results]]
value = "[{ x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ id: t:5, x: 2 }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ti:2, n: 8 }]"

[[test.results]]
value = "[{ n: 10 }]"

[[test.results]]
value = "[0, 1, 1, 2, 2, 2]"

[[test.results]]
value = "[2, 8, 10]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE ti SCHEMAFULL;
DEFINE FIELD n ON ti TYPE int VALUE $value + 1;
DEFINE INDEX ti_n ON ti FIELDS n UNIQUE;
CREATE t:1 SET x = 0;
CREATE ti:1 SET n = 1;
UPSERT t:1 SET x = 1 WHERE false;
UPSERT t:2 SET x = 1 WHERE false;
UPSERT t SET x = 1 WHERE false RETURN x;
UPSERT t:1 SET x = 2 WHERE NONE RETURN AFTER;
UPSERT t:3 SET x = 2 WHERE NULL RETURN BEFORE;
UPSERT t:4 SET x = 2 WHERE false RETURN NONE;
UPSERT ONLY t:5 SET x = 2 WHERE false;
UPSERT ti:1 SET n = 7 WHERE false;
UPSERT ti:2 SET n = 7 WHERE false;
UPSERT ti SET n = 9 WHERE false RETURN n;
SELECT VALUE x FROM t ORDER BY x;
SELECT VALUE n FROM ti ORDER BY n;
