/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test HNSW index for KNN queries with insert, update, delete, and EXPLAIN"

[[test.results]]
value = "[{ id: pts:1, point: [1, 2, 3, 4] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4, 5, 6, 7] }]"

[[test.results]]
value = "[{ id: pts:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:3, point: [8, 9, 10, 11] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'hnsw_pts', operator: '<|2,100|>', value: [2, 3, 4, 5] }, table: 'pts' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'pts' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[]"
*/

CREATE pts:1 SET point = [1,2,3,4];
CREATE pts:2 SET point = [4,5,6,7];
CREATE pts:3;
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
UPDATE pts:3 SET point = [8,9,10,11];
LET $pt = [2,3,4,5];
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt;
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt EXPLAIN;
-- Brute force for comparison (EUCLIDEAN distance type instead of threshold)
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt EXPLAIN;
DELETE pts:3;
