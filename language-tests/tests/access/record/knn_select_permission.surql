/**
[env]
namespace = true
database = true
imports = ["access/record/knn_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
issue = 474
reason = "HNSW KNN as a record user must apply the table's SELECT permission to every candidate inside the ANN search, before the residual WHERE runs (idx/trees/hnsw/filter.rs::is_record_truthy). Query 2 (residual WHERE): hidden rows (bob's) must neither be probed by the WHERE nor consume top-K slots, and alice's non-matching rows must not consume slots either -> vec:1, vec:5, vec:9. Query 3 (no WHERE): there is no per-candidate filter, so hidden rows DO consume top-K slots before the post-search permission filter drops them -> only vec:1 and vec:3 survive from the top-3 (vec:1, vec:2, vec:3). Query 4: the permission references `id`, so the per-candidate check must see the record id spliced into the candidate document -> vecb:1 (nearest) is excluded by the permission, vecb:3 by the WHERE."

[[test.results]]
value = "user:alice"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: vec:1 }, { distance: 50f, id: vec:5 }, { distance: 90f, id: vec:9 }]"

[[test.results]]
value = "[{ distance: 10f, id: vec:1 }, { distance: 30f, id: vec:3 }]"

[[test.results]]
value = "[{ distance: 20f, id: vecb:2 }, { distance: 40f, id: vecb:4 }]"
*/

RETURN $auth;
LET $q = [0f];
SELECT id, vector::distance::knn() AS distance FROM vec
	WHERE point <|3,40|> $q AND flag = true
	ORDER BY distance;
SELECT id, vector::distance::knn() AS distance FROM vec
	WHERE point <|3,40|> $q
	ORDER BY distance;
SELECT id, vector::distance::knn() AS distance FROM vecb
	WHERE point <|2,20|> $q AND flag = true
	ORDER BY distance;
