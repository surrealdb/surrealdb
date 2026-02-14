/**
[test]
reason = "Verify brute-force KNN with additional AND predicate correctly filters before computing top-K"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ active: true, distance: 2f, id: pts:3, point: [3, 0] }, { active: true, distance: 9f, id: pts:1, point: [10, 0] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2, batches: 1}
└────> SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2, batches: 1}
       └────> Compute [ctx: Db] [fields: distance = vector::distance::euclidean(...)] {rows: 2, batches: 1}
              └────> KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 2] {rows: 2, batches: 1}
                     └────> TableScan [ctx: Db] [table: pts, direction: Forward, predicate: active = true] {rows: 3, batches: 1}

Total rows: 2''''

[[test.results]]
value = "[{ active: true, distance: 2f, id: pts:3, point: [3, 0] }, { active: true, distance: 9f, id: pts:1, point: [10, 0] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2, batches: 1}
└────> SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2, batches: 1}
       └────> Compute [ctx: Db] [fields: distance = vector::distance::euclidean(...)] {rows: 2, batches: 1}
              └────> KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 2] {rows: 2, batches: 1}
                     └────> TableScan [ctx: Db] [table: pts, direction: Forward, predicate: active = true] {rows: 3, batches: 1}

Total rows: 2''''
*/
{
	CREATE pts:1 SET point = [10, 0], active = true;
	CREATE pts:2 SET point = [2, 0], active = false;
	CREATE pts:3 SET point = [3, 0], active = true;
	CREATE pts:4 SET point = [100, 0], active = true;
	CREATE pts:5 SET point = [50, 0], active = false;
	RETURN "OK";
};

-- Brute-force KNN with AND filter: only active points should be considered for top-K.
-- pts:2 (distance 1) is nearest but inactive; pts:3 (distance 2) and pts:1 (distance 9)
-- are the two nearest active points.
SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;

EXPLAIN ANALYZE SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;

-- Same query with NO INDEX hint
SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WITH NO INDEX WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;

EXPLAIN ANALYZE SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WITH NO INDEX WHERE point <|2,EUCLIDEAN|> [1, 0] AND active = true ORDER BY distance;
