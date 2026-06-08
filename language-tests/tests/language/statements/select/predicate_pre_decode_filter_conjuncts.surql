/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Structural pre-decode filter for AND conjunctions at the document root and nested shared prefixes (outer.x AND outer.y). Triple conjuncts and NOT must match full decode; scan_predicate remains authoritative. EXPLAIN shows plan-time pre_decode_filter status on KV scans."

[[test.results]]
value = "{ a: 1, b: 2, id: t:1, outer: { x: 10, y: 20 } }"

[[test.results]]
value = "{ a: 1, b: 3, id: t:2, outer: { x: 10, y: 999 } }"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = """
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: a = 1 AND b = 3, pre_decode_filter: yes]
'"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: ex_cf:1, x: 1 }]"

[[test.results]]
value = """
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ex_cf, direction: Forward, predicate: a = 1, pre_decode_filter: yes]
'"""

[[test.results]]
value = """
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: outer CONTAINS {x: 10}, pre_decode_filter: yes]
'"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: ex_fp:1 }]"

[[test.results]]
value = """
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: ex_fp, direction: Forward, predicate: a = 1, pre_decode_filter: deferred (runtime field state)]
'"""
*/

CREATE ONLY t:1 SET a = 1, b = 2, outer = { x: 10, y: 20 };
CREATE ONLY t:2 SET a = 1, b = 3, outer = { x: 10, y: 999 };

SELECT id FROM t WHERE a = 1 AND b = 3;
SELECT id FROM t WHERE a = 1 AND b = 2;
SELECT id FROM t WHERE outer.x = 10 AND outer.y = 20;
SELECT id FROM t WHERE outer.x = 10 AND outer.y = 999;
SELECT id FROM t WHERE a = 1 AND b = 2 AND outer.y = 20;
SELECT id FROM t WHERE outer.x = 10 AND outer.y = 20 AND b = 2;
SELECT id FROM t WHERE b != 3;

EXPLAIN SELECT id FROM t WHERE a = 1 AND b = 3;

DEFINE TABLE ex_cf SCHEMALESS;
DEFINE FIELD x ON ex_cf VALUE 1;
DEFINE FIELD a ON ex_cf TYPE int;
CREATE ex_cf:1 SET a = 1;
EXPLAIN SELECT id FROM ex_cf WHERE a = 1;

EXPLAIN SELECT id FROM t WITH NOINDEX WHERE outer CONTAINS { x: 10 };

DEFINE TABLE ex_fp SCHEMALESS;
DEFINE FIELD a ON ex_fp TYPE int PERMISSIONS FOR select WHERE false;
CREATE ex_fp:1 SET a = 1;
EXPLAIN SELECT * FROM ex_fp WHERE a = 1;
