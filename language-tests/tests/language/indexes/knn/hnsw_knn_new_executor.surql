/**
[test]
reason = "Test HNSW index for KNN queries with insert, update, delete, and EXPLAIN (new executor)"

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
error = "Unknown function 'vector::distance::knn' - not found in function registry"

[[test.results]]
error = "Unknown function 'vector::distance::knn' - not found in function registry"

[[test.results]]
error = "Invalid query: New executor does not support: Brute-force KNN with parameter-based vectors is not supported in the streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: Brute-force KNN with parameter-based vectors is not supported in the streaming executor"

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
EXPLAIN ANALYZE SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt;
-- Brute force for comparison (EUCLIDEAN distance type instead of threshold)
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
EXPLAIN ANALYZE SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
DELETE pts:3;
