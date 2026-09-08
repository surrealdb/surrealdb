/**
[test]

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [10f, 20f, 30f, 40f] }]"

[[test.results]]
value = "[{ id: pts:3, point: [100f, 100f, 100f, 100f] }]"

[[test.results]]
value = "NONE"

*/
CREATE pts:1 SET point = [1.0, 2.0, 3.0, 4.0];
CREATE pts:2 SET point = [10.0, 20.0, 30.0, 40.0];
CREATE pts:3 SET point = [100.0, 100.0, 100.0, 100.0];
DEFINE INDEX dann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
