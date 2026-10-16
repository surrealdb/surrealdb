/**
[env]
namespace = true
database = true

[test]
reason = """
	The largest and the smallest value of a field declared `int` and indexed, read through
	`math::max` and `math::min`: the value, its kind, the shape of the row, every way the statement
	is spelled and paged, the table as it changes, and the plan the statement is explained as.
	A `WHERE true` and an `n + 0` argument answer as the bare statements do.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[{ m: -1000000000000 }]"

[[test.results]]
value = '[{ "math::max": 1000000000000 }]'

[[test.results]]
value = '[{ "math::min": -1000000000000 }]'

[[test.results]]
value = "[1000000000000]"

[[test.results]]
value = "[-1000000000000]"

[[test.results]]
value = "[{ a: { b: 1000000000000 } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "1000000000000"

[[test.results]]
value = "-1000000000000"

[[test.results]]
value = "[{ id: t:1, m: 1000000000000 }, { id: t:2, m: 1000000000000 }, { id: t:3, m: 1000000000000 }, { id: t:4, m: 1000000000000 }, { id: t:5, m: 1000000000000 }, { id: t:6, m: 1000000000000 }, { id: t:7, m: 1000000000000 }]"

[[test.results]]
value = "[{ id: t:1, m: [{ lo: -1000000000000 }] }, { id: t:2, m: [{ lo: -1000000000000 }] }]"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ m: 1000000000000 }"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[-Infinity]"

[[test.results]]
value = "Infinity"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[{ m: -1000000000000 }]"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[{ m: -1000000000000 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 1000000000000 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 40 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 41 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 9223372036854775807 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: -9223372036854775808 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: Infinity }]"
*/

{
	DEFINE TABLE t SCHEMAFULL;
	DEFINE FIELD n ON t TYPE int;
	DEFINE INDEX n ON t FIELDS n;
	DEFINE TABLE empty SCHEMAFULL;
	DEFINE FIELD n ON empty TYPE int;
	DEFINE INDEX n ON empty FIELDS n;
	CREATE t:5 SET n = 40 RETURN NONE;
	CREATE t:1 SET n = -7 RETURN NONE;
	CREATE t:3 SET n = 1000000000000 RETURN NONE;
	CREATE t:2 SET n = -1000000000000 RETURN NONE;
	CREATE t:4 SET n = 1000000000000 RETURN NONE;
	CREATE t:6 SET n = -1000000000000 RETURN NONE;
	CREATE t:7 SET n = 0 RETURN NONE;
	RETURN 'OK';
};

-- The extremes, with an alias, a derived name and a bare value.
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
SELECT math::max(n) FROM t GROUP ALL;
SELECT math::min(n) FROM t GROUP ALL;
SELECT VALUE math::max(n) FROM t GROUP ALL;
SELECT VALUE math::min(n) FROM t GROUP ALL;
SELECT math::max(n) AS a.b FROM t GROUP ALL;

-- The kind of the answer.
RETURN type::is_int((SELECT VALUE math::max(n) FROM t GROUP ALL)[0]);
RETURN type::is_int((SELECT VALUE math::min(n) FROM t GROUP ALL)[0]);

-- In value position.
RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t GROUP ALL)[0];
SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] AS m FROM t ORDER BY id;
SELECT id, (SELECT math::min(n) AS lo FROM t GROUP ALL) AS m FROM t ORDER BY id LIMIT 2;
SELECT VALUE id FROM t WHERE n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] ORDER BY id;

-- Paging the one row.
SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 1;
SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 0;
SELECT math::max(n) AS m FROM t GROUP ALL START 1;
SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 1 START 0;
SELECT VALUE math::min(n) FROM t GROUP ALL LIMIT 1 START 1;
SELECT math::max(n) AS m FROM ONLY t GROUP ALL;

-- A table with no rows.
SELECT math::max(n) AS m FROM empty GROUP ALL;
SELECT math::min(n) AS m FROM empty GROUP ALL;
SELECT VALUE math::max(n) FROM empty GROUP ALL;
RETURN (SELECT VALUE math::min(n) FROM empty GROUP ALL)[0];

-- A statement the plan for an indexed field does not apply to answers as the plan does.
SELECT math::max(n) AS m FROM t WHERE true GROUP ALL;
SELECT math::min(n) AS m FROM t WHERE true GROUP ALL;
SELECT math::max(n + 0) AS m FROM t GROUP ALL;
SELECT math::min(n + 0) AS m FROM t GROUP ALL;

-- The statements as they are explained.
EXPLAIN SELECT math::max(n) AS m FROM t GROUP ALL;
EXPLAIN SELECT math::min(n) AS m FROM t GROUP ALL;
EXPLAIN SELECT VALUE math::max(n) FROM t GROUP ALL;

-- The table as it changes: the extreme row leaves, the duplicates leave, a new extreme arrives.
DELETE t:3 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
DELETE t:4 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
UPDATE t:5 SET n = 41 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
CREATE t:8 SET n = 9223372036854775807 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
CREATE t:9 SET n = -9223372036854775808 RETURN NONE;
SELECT math::min(n) AS m FROM t GROUP ALL;
DELETE t RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
