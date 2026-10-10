/**
[env]

[test]
reason = "Brute-force KNN on a table without a vector index flattens a record's nested vector arrays in order, as a vector index stores them; a record holding anything else in its vector is skipped"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 1f, id: pts:1 }, { d: 2f, id: pts:3 }, { d: 10f, id: pts:2 }]"
*/
{
	CREATE pts:1 SET point = [[0f, 0f], [0f, 1f]];
	CREATE pts:2 SET point = [5f, 5f, 5f, 5f];
	CREATE pts:3 SET point = [[1f], [1f, 1f], [1f]];
	CREATE pts:4 SET point = [[1f, 'x'], [1f, 1f]];
	CREATE pts:5 SET point = [[], []];
	RETURN NONE;
};
SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|5,EUCLIDEAN|> [0f, 0f, 0f, 0f] ORDER BY d, id;
