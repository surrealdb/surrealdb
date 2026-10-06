/**
[env]
namespace = true
database = true
backend = ["rocksdb", "surrealkv"]
versioned = true
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["reproductions/7555_knn_permission_gate_reads_at_the_query_version_import.surql"]

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
	The SELECT-permission gate a KNN applies to each candidate inside the ANN
	search reads the candidate at the query's VERSION, the same revision the
	returned rows are read at. vec:1 was alice's at $ts and is bob's now: a
	versioned KNN at $ts returns it, and the current one does not. Pinned to
	the streaming strategies: the per-row engine applies no in-search gate to
	a KNN with no other condition (7555_knn_returns_k_rows_the_caller_can_see_legacy.surql).
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vec:1, vec:3]"

[[test.results]]
value = "[vec:3, vec:5]"
*/

LET $ts = marker:ts.at;
LET $q = [0f];
SELECT VALUE id FROM vec WHERE point <|2,40|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM vec WHERE point <|2,40|> $q ORDER BY id;
