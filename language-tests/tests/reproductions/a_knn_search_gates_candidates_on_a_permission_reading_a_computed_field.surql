/**
[env]
namespace = true
database = true
imports = ["reproductions/a_knn_search_gates_candidates_on_a_permission_reading_a_computed_field_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
	A KNN search run by a record user whose table SELECT permission reads a
	COMPUTED field is lowered with the permission judged inside the ANN
	search, over the computed value: hidden rows take no top-k slot, so the
	user gets the k nearest rows they can see, on HNSW and DiskANN alike.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dbl: 42, distance: 21f, id: hnsw_c:21 }, { dbl: 44, distance: 22f, id: hnsw_c:22 }, { dbl: 46, distance: 23f, id: hnsw_c:23 }]"

[[test.results]]
value = "[{ dbl: 42, distance: 21f, id: diskann_c:21 }, { dbl: 44, distance: 22f, id: diskann_c:22 }, { dbl: 46, distance: 23f, id: diskann_c:23 }]"

[[test.results]]
value = "[{ dbl: 42, emb: [21f], id: hnsw_c:21, n: 21 }, { dbl: 44, emb: [22f], id: hnsw_c:22, n: 22 }]"

[[test.results]]
value = "[hnsw_c:21, hnsw_c:23, hnsw_c:24]"

[[test.results]]
value = "[diskann_c:21, diskann_c:23, diskann_c:24]"

[[test.results]]
value = "[{ count: 5 }]"
*/

RETURN $auth;
LET $q = [0f];
SELECT id, dbl, vector::distance::knn() AS distance FROM hnsw_c
	WHERE emb <|3,40|> $q
	ORDER BY distance;
SELECT id, dbl, vector::distance::knn() AS distance FROM diskann_c
	WHERE emb <|3,40|> $q
	ORDER BY distance;
SELECT * FROM hnsw_c WHERE emb <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM hnsw_c WHERE emb <|3,40|> $q AND n != 22 ORDER BY id;
SELECT VALUE id FROM diskann_c WHERE emb <|3,40|> $q AND n != 22 ORDER BY id;
SELECT count() FROM hnsw_c WHERE emb <|5,40|> $q GROUP ALL;
