/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "Static array/object record-ID keys and bare object literals must be converted into index lookup values without leaving a residual filter"
issue = 7496

[[test.results]]
value = "'OK'"

[[test.results]]
value = """
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: t_v, access: = x:[1, 'a'], direction: Forward]
""""

[[test.results]]
value = "[t:2]"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    IndexScan [ctx: Db] [index: t_v, access: = x:{ k: 1 }, direction: Forward]\n''''

[[test.results]]
value = "[t:3]"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    IndexScan [ctx: Db] [index: t_v, access: = { k: 1 }, direction: Forward]\n''''

[[test.results]]
value = "[t:4]"

[[test.results]]
value = """
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: t_v, access: = x:[1, 'a'], direction: Forward]
""""

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    IndexScan [ctx: Db] [index: t_v, access: = x:{ k: 1 }, direction: Forward]\n''''

[[test.results]]
value = "[t:3]"
*/

{
	DEFINE TABLE t SCHEMALESS;
	DEFINE INDEX t_v ON t FIELDS v;
	CREATE t:1 SET v = x:plain;
	CREATE t:2 SET v = x:[1, 'a'];
	CREATE t:3 SET v = x:{ k: 1 };
	CREATE t:4 SET v = { k: 1 };
	RETURN 'OK';
};

-- Array record-ID keys must be materialized into index lookup values.
EXPLAIN SELECT VALUE id FROM t WHERE v = x:[1, 'a'];
SELECT VALUE id FROM t WHERE v = x:[1, 'a'];

-- Object record-ID keys and bare objects follow the same literal-only path.
EXPLAIN SELECT VALUE id FROM t WHERE v = x:{ k: 1 };
SELECT VALUE id FROM t WHERE v = x:{ k: 1 };
EXPLAIN SELECT VALUE id FROM t WHERE v = { k: 1 };
SELECT VALUE id FROM t WHERE v = { k: 1 };

-- Single-element IN is normalized to equality by both index analysis and
-- residual-filter stripping.
EXPLAIN SELECT VALUE id FROM t WHERE v IN [x:[1, 'a']];
SELECT VALUE id FROM t WHERE v IN [x:[1, 'a']];

-- Resolved parameters must use the same static conversion path.
LET $p = x:{ k: 1 };
EXPLAIN SELECT VALUE id FROM t WHERE v = $p;
SELECT VALUE id FROM t WHERE v = $p;
