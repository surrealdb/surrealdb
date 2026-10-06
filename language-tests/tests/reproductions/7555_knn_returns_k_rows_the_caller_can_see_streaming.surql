/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]
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
	A divergence, pinned in a pair rather than fixed: a KNN operator with no
	other condition in its WHERE, run by a record user whose table SELECT
	permission hides rows, must return the k nearest neighbours that user can
	see. The streaming engine applies the permission to every candidate inside
	the ANN search (HNSW and DiskANN alike), so hidden rows never take a top-k
	slot and alice gets her three nearest rows. The per-row engine applies the
	permission to the k rows the index already chose, which are all bob's, and
	returns none. The streaming answer is correct; the per-row engine is being
	retired and is not fixed here.
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
