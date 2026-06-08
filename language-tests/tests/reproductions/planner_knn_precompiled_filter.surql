/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Brute-force KNN must apply precompiled residual filters before top-K"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ active: true, category: 'x', distance: 1f, id: pts:2, point: [2, 0], tag: 'n' }, { active: true, category: 'y', distance: 2f, id: pts:3, point: [3, 0], tag: 'x' }]"
*/
{
	DEFINE INDEX idx_category ON pts FIELDS category;
	DEFINE INDEX idx_tag ON pts FIELDS tag;
	CREATE pts:1 SET point = [1.1, 0], category = "x", tag = "n", active = false;
	CREATE pts:2 SET point = [2, 0], category = "x", tag = "n", active = true;
	CREATE pts:3 SET point = [3, 0], category = "y", tag = "x", active = true;
	CREATE pts:4 SET point = [4, 0], category = "y", tag = "n", active = true;
	RETURN "OK";
};

SELECT *, vector::distance::euclidean(point, [1, 0]) AS distance
FROM pts
WHERE point <|2,EUCLIDEAN|> [1, 0]
	AND ((category = "x" AND active = true) OR (tag = "x" AND active = true))
ORDER BY distance;
