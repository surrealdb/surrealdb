/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "DiskANN KNN query with an additional WHERE condition (new executor). The residual non-KNN predicate is pushed into the ANN search and rejects non-matching candidates during traversal, so the two nearest `flag = true` points are returned (not the two nearest overall). EXPLAIN surfaces the pushed-down filter as a `predicate` on the KnnScan line. DiskANN twin of hnsw_knn_with_condition_new_executor.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ flag: true, id: pts:1, point: [10f] }, { flag: false, id: pts:2, point: [20f] }, { flag: true, id: pts:3, point: [30f] }, { flag: false, id: pts:4, point: [40f] }, { flag: true, id: pts:5, point: [50f] }, { flag: false, id: pts:6, point: [60f] }, { flag: true, id: pts:7, point: [70f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id, flag, distance] {rows: 2}\n    SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2}\n        Compute [ctx: Db] [fields: distance = vector::distance::knn(...)] {rows: 2}\n            Filter [ctx: Db] [predicate: flag = true] {rows: 2}\n                KnnScan [ctx: Db] [index: da_pt1, k: 2, ef: 40, dimension: 1, predicate: flag = true] {rows: 2}\n\nTotal rows: 2''''

[[test.results]]
value = "[{ distance: 6f, flag: true, id: pts:5 }, { distance: 14f, flag: true, id: pts:3 }]"
*/

DEFINE INDEX da_pt1 ON pts FIELDS point DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], flag: true },
	{ id: pts:2, point: [ 20f ], flag: false },
	{ id: pts:3, point: [ 30f ], flag: true },
	{ id: pts:4, point: [ 40f ], flag: false },
	{ id: pts:5, point: [ 50f ], flag: true },
	{ id: pts:6, point: [ 60f ], flag: false },
	{ id: pts:7, point: [ 70f ], flag: true }
];
LET $pt = [44f];
EXPLAIN ANALYZE SELECT id, flag, vector::distance::knn() AS distance FROM pts
	WHERE flag = true AND point <|2,40|> $pt
	ORDER BY distance;
SELECT id, flag, vector::distance::knn() AS distance FROM pts
	WHERE flag = true AND point <|2,40|> $pt
	ORDER BY distance;
