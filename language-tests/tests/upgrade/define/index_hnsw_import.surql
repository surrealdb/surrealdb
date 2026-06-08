/**
[test]

[[test.results]]
value = "[{ id: pts:1, point: [1, 2, 3, 4] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4, 5, 6, 7] }]"

[[test.results]]
value = "[{ id: pts:3, point: [8, 9, 10, 11] }]"

[[test.results]]
value = "[{ id: pts:4, point: [50, 50, 50, 50] }]"

[[test.results]]
value = "NONE"

*/
CREATE pts:1 SET point = [1, 2, 3, 4];
CREATE pts:2 SET point = [4, 5, 6, 7];
CREATE pts:3 SET point = [8, 9, 10, 11];
CREATE pts:4 SET point = [50, 50, 50, 50];
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
