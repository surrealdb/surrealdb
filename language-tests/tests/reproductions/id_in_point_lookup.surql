
/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "WHERE id IN [RecordId, ...] and [RecordId, ...] CONTAINS id should use RecordIdScan point lookups instead of a full table scan"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    Union [ctx: Db]
        RecordIdScan [ctx: Db] [record_id: item:1]
        RecordIdScan [ctx: Db] [record_id: item:2]
''''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    RecordIdScan [ctx: Db] [record_id: item:1]
''''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    Union [ctx: Db]
        RecordIdScan [ctx: Db] [record_id: item:1]
        RecordIdScan [ctx: Db] [record_id: item:2]
''''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    Union [ctx: Db]
        RecordIdScan [ctx: Db] [record_id: item:1, predicate: score > 15]
        RecordIdScan [ctx: Db] [record_id: item:2, predicate: score > 15]
''''

[[test.results]]
value = "[{ id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    EmptyResult [ctx: Rt]
''''

*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- Multi-element id IN: should use Union of RecordIdScans
EXPLAIN SELECT * FROM item WHERE id IN [item:1, item:2];

-- Verify correct results
SELECT * FROM item WHERE id IN [item:1, item:2];

-- Single-element id IN: should use RecordIdScan directly
EXPLAIN SELECT * FROM item WHERE id IN [item:1];

-- Verify correct result
SELECT * FROM item WHERE id IN [item:1];

-- CONTAINS variant: should also use Union of RecordIdScans
EXPLAIN SELECT * FROM item WHERE [item:1, item:2] CONTAINS id;

-- Verify correct results
SELECT * FROM item WHERE [item:1, item:2] CONTAINS id;

-- Residual predicate: RecordIdScan with full predicate
EXPLAIN SELECT * FROM item WHERE id IN [item:1, item:2] AND score > 15;

-- Verify correct result (only item:2 passes the residual)
SELECT * FROM item WHERE id IN [item:1, item:2] AND score > 15;

-- Empty array: should return empty (no optimisation, falls through)
SELECT * FROM item WHERE id IN [];

-- Empty array: should return empty (no optimisation, falls through)
EXPLAIN SELECT * FROM item WHERE id IN [];
