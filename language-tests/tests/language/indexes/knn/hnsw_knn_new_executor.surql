/**
[test]
reason = "Verify HNSW index-backed KNN search works in the new streaming executor"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: pts:2, point: [2, 0, 0, 0] }, { id: pts:3, point: [3, 0, 0, 0] }]"

*/
{
	DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
	CREATE pts:1 SET point = [10, 0, 0, 0];
	CREATE pts:2 SET point = [2, 0, 0, 0];
	CREATE pts:3 SET point = [3, 0, 0, 0];
	CREATE pts:4 SET point = [100, 0, 0, 0];
	CREATE pts:5 SET point = [50, 0, 0, 0];
	RETURN "OK";
};

-- HNSW KNN: find 2 nearest neighbors to [1, 0, 0, 0]
SELECT * FROM pts WHERE point <|2,40|> [1, 0, 0, 0];
