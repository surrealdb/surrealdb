/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "EXPLAIN plans show plan-time source resolution: type::table($name) resolves to concrete operators"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_ts, access: = d'2025-01-05T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_ts, access: , direction: Forward, limit: 3]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: item, direction: Forward, limit: 3]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_ts, access: , direction: Backward, limit: 3]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: item, direction: Forward, limit: 3]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_ts, access: , direction: Forward, limit: 3]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: item, direction: Forward, limit: 3]
''''
*/

DEFINE INDEX idx_ts ON item FIELDS ts;
FOR $i IN 1..=10 {
    CREATE type::record('item', $i) SET ts = d'2025-01-01T00:00:00Z' + duration::from_days($i - 1);
};

LET $table = type::table("item");
LET $name = "item";

-- === $param (Value::Table) resolves to plan-time operators ===

-- Index equality: $param gets IndexScan, same as literal
EXPLAIN SELECT * FROM $table WHERE ts = d'2025-01-05T00:00:00Z';

-- ORDER BY indexed field + LIMIT: IndexScan with limit pushed + sort eliminated
EXPLAIN SELECT * FROM $table ORDER BY ts ASC LIMIT 3;

-- ORDER BY id + LIMIT: TableScan with limit pushed (id-compatible)
EXPLAIN SELECT * FROM $table ORDER BY id LIMIT 3;

-- ORDER BY indexed field DESC + LIMIT: IndexScan backward with limit pushed + sort eliminated
EXPLAIN SELECT * FROM $table ORDER BY ts DESC LIMIT 3;

-- === type::table($name) resolved at plan time to concrete operators ===

-- ORDER BY id + LIMIT: resolves to TableScan with limit pushed
EXPLAIN SELECT id FROM type::table($name) ORDER BY id LIMIT 3;

-- ORDER BY indexed field + LIMIT: resolves to IndexScan with limit pushed + sort eliminated
EXPLAIN SELECT * FROM type::table($name) ORDER BY ts ASC LIMIT 3;

-- No ORDER BY + LIMIT: resolves to TableScan with limit pushed
EXPLAIN SELECT id FROM type::table($name) LIMIT 3;
