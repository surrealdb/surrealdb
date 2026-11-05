/**
[env]
namespace = true
database = true
imports = ["reproductions/7555_knn_returns_k_rows_the_caller_can_see_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
issue = 7555
reason = """
	A KNN operator with no other condition in its WHERE, run by a record user
	whose table SELECT permission hides rows, returns the k nearest neighbours
	that user can see. The permission is applied to every candidate inside the
	ANN search (HNSW and DiskANN alike), so hidden rows never take a top-k slot
	and alice gets her three nearest rows, even though every one of the
	table's k nearest neighbours is bob's.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 21f, id: hnsw_chunk:21 }, { distance: 22f, id: hnsw_chunk:22 }, { distance: 23f, id: hnsw_chunk:23 }]"

[[test.results]]
value = "[{ distance: 21f, id: diskann_chunk:21 }, { distance: 22f, id: diskann_chunk:22 }, { distance: 23f, id: diskann_chunk:23 }]"
*/

RETURN $auth;
LET $q = [0f];
SELECT id, vector::distance::knn() AS distance FROM hnsw_chunk
	WHERE emb <|3,40|> $q
	ORDER BY distance;
SELECT id, vector::distance::knn() AS distance FROM diskann_chunk
	WHERE emb <|3,40|> $q
	ORDER BY distance;
