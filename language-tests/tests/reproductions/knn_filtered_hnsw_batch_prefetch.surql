/**
[env]

[test]
reason = "Filtered HNSW KNN warms the transaction record cache for each visited neighbourhood in one batch before evaluating the residual WHERE (idx/trees/hnsw/layer.rs::prefetch_neighbourhood_records + filter.rs::prefetch_records). The evaluation loop is left byte-identical, so a selective residual filter must return exactly the same rows, in the same order, as before. Query vector [0f]; category = 'a' selects the odd-id points (10, 30, 50, ...); the three nearest matching rows are pts:1, pts:3, pts:5 — note pts:2 (distance 20) is nearer than pts:3 (distance 30) but is correctly excluded by the filter."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 30f, id: pts:3 }, { distance: 50f, id: pts:5 }]"
*/

{
	DEFINE INDEX pt_hnsw ON pts FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
	INSERT INTO pts [
		{ id: pts:1,  point: [ 10f ],  category: "a" },
		{ id: pts:2,  point: [ 20f ],  category: "b" },
		{ id: pts:3,  point: [ 30f ],  category: "a" },
		{ id: pts:4,  point: [ 40f ],  category: "b" },
		{ id: pts:5,  point: [ 50f ],  category: "a" },
		{ id: pts:6,  point: [ 60f ],  category: "b" },
		{ id: pts:7,  point: [ 70f ],  category: "a" },
		{ id: pts:8,  point: [ 80f ],  category: "b" },
		{ id: pts:9,  point: [ 90f ],  category: "a" },
		{ id: pts:10, point: [ 100f ], category: "b" },
		{ id: pts:11, point: [ 110f ], category: "a" },
		{ id: pts:12, point: [ 120f ], category: "b" }
	];
	RETURN 'OK';
};
LET $q = [0f];
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE point <|3,40|> $q AND category = "a"
	ORDER BY distance;
