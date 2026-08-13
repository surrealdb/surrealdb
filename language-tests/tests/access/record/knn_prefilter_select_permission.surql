/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["access/record/knn_prefilter_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
issue = 548
reason = "Pre-filtered KNN as a record user: the allow-list bitmap is built from index entries and so contains permission-hidden (bob's) rows — row permissions are never bitmap-evaluated. The per-record SELECT permission must still exclude them on every tier without letting them consume top-K slots: the covered `flag = true` query returns alice's three flagged rows (vec:1, vec:5, vec:9) even though bob's flagged rows sit nearer, identical to the pre-#548 residual-path behaviour pinned by knn_select_permission.surql."

[[test.results]]
value = "user:alice"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: vec:1 }, { distance: 50f, id: vec:5 }, { distance: 90f, id: vec:9 }]"

[[test.results]]
value = "[{ distance: 10f, id: vec:1 }, { distance: 30f, id: vec:3 }]"
*/

RETURN $auth;
LET $q = [0f];
-- Fully-covered WHERE: the bitmap admits all flagged rows (alice's AND
-- bob's); the permission gate excludes bob's before any top-K slot is
-- consumed.
SELECT id, vector::distance::knn() AS distance FROM vec
	WHERE point <|3,40|> $q AND flag = true
	ORDER BY distance;
-- No WHERE: no prefilter and no per-candidate filter — hidden rows still
-- consume top-K slots before the post-search permission filter drops them
-- (pre-existing semantics, unchanged by #548).
SELECT id, vector::distance::knn() AS distance FROM vec
	WHERE point <|3,40|> $q
	ORDER BY distance;
