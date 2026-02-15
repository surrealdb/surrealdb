/**
[test]
reason = "Verify NOT (field INSIDE [...]) is NOT expanded into index lookups and returns correct results"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: items, direction: Forward, predicate: ! val INSIDE [1, 2]] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: items:3, val: 3 }, { id: items:4, val: 4 }]"

*/
{
	DEFINE INDEX idx_val ON items FIELDS val;
	CREATE items:1 SET val = 1;
	CREATE items:2 SET val = 2;
	CREATE items:3 SET val = 3;
	CREATE items:4 SET val = 4;
	RETURN "OK";
};

-- NOT INSIDE must NOT use UnionIndexScan (would return wrong results).
-- It should return rows OUTSIDE the set [1, 2].
EXPLAIN ANALYZE SELECT * FROM items WHERE !(val INSIDE [1, 2]);
SELECT * FROM items WHERE !(val INSIDE [1, 2]);
