/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "HNSW index with DIST COSINE returns neighbours in ascending cosine-distance order. Guards the cached stored-vector norm path: an incorrect norm would corrupt the cosine ranking. Points are chosen so cosine distances to [1,0,0] are strictly increasing (pts:1<pts:2<pts:3<pts:4<pts:5) with no ties."

[[test.results]]
value = "[{ id: pts:1, point: [2, 0, 0] }]"

[[test.results]]
value = "[{ id: pts:2, point: [3, 1, 0] }]"

[[test.results]]
value = "[{ id: pts:3, point: [1, 1, 0] }]"

[[test.results]]
value = "[{ id: pts:4, point: [1, 3, 0] }]"

[[test.results]]
value = "[{ id: pts:5, point: [0, 1, 0] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:3 }, { id: pts:4 }, { id: pts:5 }]"

[[test.results]]
value = "[{ dist: 0f, id: pts:1 }, { dist: 0.05131670194948623f, id: pts:2 }, { dist: 0.29289321881345254f, id: pts:3 }, { dist: 0.683772233983162f, id: pts:4 }, { dist: 1f, id: pts:5 }]"
*/

CREATE pts:1 SET point = [2, 0, 0];
CREATE pts:2 SET point = [3, 1, 0];
CREATE pts:3 SET point = [1, 1, 0];
CREATE pts:4 SET point = [1, 3, 0];
CREATE pts:5 SET point = [0, 1, 0];
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 3 DIST COSINE TYPE F64 EFC 500 M 12;
LET $pt = [1, 0, 0];
SELECT id FROM pts WHERE point <|5,100|> $pt;
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|5,100|> $pt;
