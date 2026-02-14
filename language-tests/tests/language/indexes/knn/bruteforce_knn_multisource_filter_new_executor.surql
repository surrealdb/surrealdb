/**
[test]
reason = "Verify brute-force KNN with AND predicate on multiple FROM sources applies filter before top-K"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ active: true, distance: 2f, id: pts:3, point: [3, 0] }, { active: true, distance: 3f, id: pts2:2, point: [4, 0] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2, batches: 1}
└────> SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2, batches: 1}
       └────> Compute [ctx: Db] [fields: distance = vector::distance::euclidean(...)] {rows: 2, batches: 1}
              └────> KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 2] {rows: 2, batches: 1}
                     └────> Filter [ctx: Db] [predicate: active = true] {rows: 3, batches: 2}
                            └────> Union [ctx: Db] {rows: 5, batches: 2}
                                   ├────> Scan [ctx: Db] [source: pts] {rows: 3, batches: 1}
                                   │        actual: [access_method: TableScan, direction: Forward]
                                   └────> Scan [ctx: Db] [source: pts2] {rows: 2, batches: 1}
                                            actual: [access_method: TableScan, direction: Forward]

Total rows: 2''''
*/
{
	-- Table 1: mix of active and inactive points
	CREATE pts:1 SET point = [10, 0], active = true;
	CREATE pts:2 SET point = [2, 0], active = false;
	CREATE pts:3 SET point = [3, 0], active = true;
	-- Table 2: additional points
	CREATE pts2:1 SET point = [1.5, 0], active = false;
	CREATE pts2:2 SET point = [4, 0], active = true;
	RETURN "OK";
};

-- Multi-source KNN with AND filter.
-- Without the fix, KnnTopK would pick top-2 from ALL rows first
-- (pts2:1 dist 0.5 inactive, pts:2 dist 1 inactive), then Filter
-- removes them both, yielding wrong/empty results.
-- With the fix, Filter runs first (keeping only active rows), then
-- KnnTopK picks top-2 from active rows: pts:3 (dist 2) and pts2:2 (dist 3).
SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts, pts2 WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;

EXPLAIN ANALYZE SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts, pts2 WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;
