/**
[test]
reason = "WHERE id = record_id should use RecordIdScan instead of a full table scan"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]
└────> RecordIdScan [ctx: Db] [record_id: item:1, predicate: id = item:1]
''''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> RecordIdScan [ctx: Db] [record_id: item:2, predicate: id = item:2 AND score > 15]
''''

[[test.results]]
value = "[{ id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> TableScan [ctx: Db] [table: item, direction: Forward, predicate: id = item:1 OR score > 15]
''''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }, { id: item:3, name: 'Gamma', score: 30 }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> RecordIdScan [ctx: Db] [record_id: item:3, predicate: item:3 = id]
''''

[[test.results]]
value = "[{ id: item:3, name: 'Gamma', score: 30 }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> RecordIdScan [ctx: Db] [record_id: item:1, predicate: id == item:1]
''''

*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- Simple id equality: should use RecordIdScan
EXPLAIN SELECT * FROM item WHERE id = item:1;

-- Verify correct result
SELECT * FROM item WHERE id = item:1;

-- id equality with AND: should use RecordIdScan with predicate
EXPLAIN SELECT * FROM item WHERE id = item:2 AND score > 15;

-- Verify correct result (item:2 has score=20, passes filter)
SELECT * FROM item WHERE id = item:2 AND score > 15;

-- id equality with AND where residual fails: should return empty
SELECT * FROM item WHERE id = item:1 AND score > 15;

-- id equality with OR: should NOT use RecordIdScan (full scan required)
EXPLAIN SELECT * FROM item WHERE id = item:1 OR score > 15;

-- Verify correct result (OR case)
SELECT * FROM item WHERE id = item:1 OR score > 15;

-- Reversed equality: record_id = id should also work
EXPLAIN SELECT * FROM item WHERE item:3 = id;

-- Verify correct result
SELECT * FROM item WHERE item:3 = id;

-- Exact equality (==) should also work
EXPLAIN SELECT * FROM item WHERE id == item:1;
