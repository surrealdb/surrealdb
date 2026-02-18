/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test HNSW KNN query with additional WHERE condition filter"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ flag: true, id: pts:1, point: [10f] }, { flag: false, id: pts:2, point: [20f] }, { flag: true, id: pts:3, point: [30f] }, { flag: false, id: pts:4, point: [40f] }, { flag: true, id: pts:5, point: [50f] }, { flag: false, id: pts:6, point: [60f] }, { flag: true, id: pts:7, point: [70f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'hn_pt1', operator: '<|2,40|>', value: [44f] }, table: 'pts' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ distance: 6f, flag: true, id: pts:5 }, { distance: 14f, flag: true, id: pts:3 }]"
*/

DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
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
SELECT id, flag, vector::distance::knn() AS distance FROM pts
	WHERE flag = true AND point <|2,40|> $pt
	ORDER BY distance EXPLAIN;
SELECT id, flag, vector::distance::knn() AS distance FROM pts
	WHERE flag = true AND point <|2,40|> $pt
	ORDER BY distance;
