/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = "KNN operator <|K, DISTANCE|> should use HNSW index instead of falling back to brute-force scan"
issue = 7229

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4f, 5f, 6f, 7f] }]"

[[test.results]]
value = "[{ id: pts:3, point: [8f, 9f, 10f, 11f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"
*/

CREATE pts:1 SET point = [1f,2f,3f,4f];
CREATE pts:2 SET point = [4f,5f,6f,7f];
CREATE pts:3 SET point = [8f,9f,10f,11f];
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
LET $pt = [2f,3f,4f,5f];

-- Approximate form (should use HNSW index) - baseline
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt;

-- K with explicit EUCLIDEAN distance (should also use HNSW when distance matches)
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
