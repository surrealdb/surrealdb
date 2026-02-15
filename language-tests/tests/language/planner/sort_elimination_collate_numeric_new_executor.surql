/**
[test]
reason = "Sort elimination must not skip COLLATE/NUMERIC modifiers on ORDER BY terms"

[[test.results]]
value = "[{ id: test:1, val: '10' }]"

[[test.results]]
value = "[{ id: test:2, val: '2' }]"

[[test.results]]
value = "[{ id: test:3, val: '20' }]"

[[test.results]]
value = "[{ id: test:4, val: '1' }]"

[[test.results]]
value = "[{ id: test:5, val: '3' }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: val ASC]
       └────> TableScan [ctx: Db] [table: test, direction: Forward]
''''

[[test.results]]
value = "[{ val: '1' }, { val: '2' }, { val: '3' }, { val: '10' }, { val: '20' }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: val ASC]
       └────> TableScan [ctx: Db] [table: test, direction: Forward]
''''

[[test.results]]
value = "[{ val: '1' }, { val: '10' }, { val: '2' }, { val: '20' }, { val: '3' }]"

[[test.results]]
value = "[{ val: '1' }, { val: '2' }, { val: '3' }]"

*/

-- Create records whose id ordering differs from NUMERIC ordering
CREATE test:1 SET val = '10';
CREATE test:2 SET val = '2';
CREATE test:3 SET val = '20';
CREATE test:4 SET val = '1';
CREATE test:5 SET val = '3';
-- Raw id ASC scan returns 1,2,3,4,5 — val in raw order: 10,2,20,1,3
-- ORDER BY val NUMERIC should sort numerically: 1,2,3,10,20

-- Verify NUMERIC ordering is NOT eliminated (Sort must appear in EXPLAIN)
EXPLAIN SELECT * FROM test ORDER BY val NUMERIC ASC;
-- Verify correct NUMERIC result ordering
SELECT val FROM test ORDER BY val NUMERIC ASC;

-- Verify COLLATE ordering is NOT eliminated (Sort must appear in EXPLAIN)
EXPLAIN SELECT * FROM test ORDER BY val COLLATE ASC;
-- Verify correct result ordering with COLLATE
SELECT val FROM test ORDER BY val COLLATE ASC;

-- Verify NUMERIC + LIMIT is not incorrectly optimized
SELECT val FROM test ORDER BY val NUMERIC ASC LIMIT 3;
