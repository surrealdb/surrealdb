/**
[test]
reason = "Verify IN with arrays larger than the expansion limit (32) falls back to TableScan instead of creating excessive UnionIndexScan branches"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *] {rows: 2}
    TableScan [ctx: Db] [table: items, direction: Forward, predicate: val INSIDE [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: items:1, val: 1 }, { id: items:2, val: 5 }]"

*/
{
	DEFINE INDEX idx_val ON items FIELDS val;
	CREATE items:1 SET val = 1;
	CREATE items:2 SET val = 5;
	CREATE items:3 SET val = 50;
	RETURN "OK";
};

-- 33 elements exceeds MAX_IN_EXPANSION_SIZE (32), should use TableScan not UnionIndexScan
EXPLAIN ANALYZE SELECT * FROM items WHERE val IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33];
SELECT * FROM items WHERE val IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33];
