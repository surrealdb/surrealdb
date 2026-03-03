/**
[test]
reason = "Verify brute-force KNN (NearestNeighbor::K) works in the new streaming executor"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ distance: 1f, id: pts:2, point: [2, 0] }, { distance: 2f, id: pts:3, point: [3, 0] }]"

[[test.results]]
value = "[{ distance: 1f, id: pts:2, point: [2, 0] }, { distance: 2f, id: pts:3, point: [3, 0] }]"

*/
{
	CREATE pts:1 SET point = [10, 0];
	CREATE pts:2 SET point = [2, 0];
	CREATE pts:3 SET point = [3, 0];
	CREATE pts:4 SET point = [100, 0];
	CREATE pts:5 SET point = [50, 0];
	RETURN "OK";
};

-- Brute-force KNN: find 2 nearest neighbors to [1, 0] using Euclidean distance
SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WHERE point <|2,EUCLIDEAN|> [1, 0] ORDER BY distance;

-- Same query with NO INDEX hint (should produce identical results)
SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance FROM pts WITH NO INDEX WHERE point <|2,EUCLIDEAN|> [1, 0] ORDER BY distance;
