/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Param holding a Table value gets full plan-time index resolution, matching literal table behavior"

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
value = '''"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_ts, access: = d'2025-01-05T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = "[item:5]"

[[test.results]]
value = "[item:5]"

[[test.results]]
value = "[item:10, item:9, item:8]"

[[test.results]]
value = "[item:10, item:9, item:8]"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:1, item:2, item:3]"
*/

DEFINE INDEX idx_ts ON item FIELDS ts;
FOR $i IN 1..=10 {
    CREATE type::record('item', $i) SET ts = d'2025-01-01T00:00:00Z' + duration::from_days($i - 1);
};

LET $table = type::table("item");

-- $param uses index, same as literal table
EXPLAIN SELECT * FROM $table WHERE ts = d'2025-01-05T00:00:00Z';
EXPLAIN SELECT * FROM item WHERE ts = d'2025-01-05T00:00:00Z';

-- Verify results match
(SELECT id FROM $table WHERE ts = d'2025-01-05T00:00:00Z').id;
(SELECT id FROM item WHERE ts = d'2025-01-05T00:00:00Z').id;

-- ORDER BY non-id + LIMIT: param and literal must return same results
(SELECT * FROM $table ORDER BY ts DESC LIMIT 3).id;
(SELECT * FROM item ORDER BY ts DESC LIMIT 3).id;

-- ORDER BY id + LIMIT: limit pushdown is valid here for both paths
(SELECT id FROM $table ORDER BY id LIMIT 3).id;
(SELECT id FROM item ORDER BY id LIMIT 3).id;
