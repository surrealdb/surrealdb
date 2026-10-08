/**
[env]
namespace = true
database = true
backend = ["rocksdb", "surrealkv"]
versioned = true
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
	A KNN returns the k nearest rows the caller may SELECT, as of the revision
	the statement reads. vec:1 was alice's at $ts and is bob's now: a KNN at
	$ts returns it, and the current one does not. The versioned search ranks
	the records read at the stamp, since an ANN index describes the present,
	and judges the table's SELECT permission on each of them at the stamp; the
	current search gates every candidate on the permission inside the ANN
	search. Each equals its WITH NOINDEX brute-force twin, which ranks the
	rows the permission leaves.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vec:1, vec:3]"

[[test.results]]
value = "[vec:1, vec:3]"

[[test.results]]
value = "[vec:3, vec:5]"

[[test.results]]
value = "[vec:3, vec:5]"
*/

LET $ts = marker:ts.at;
LET $q = [0f];
SELECT VALUE id FROM vec WHERE point <|2,40|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM vec WITH NOINDEX WHERE point <|2,EUCLIDEAN|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM vec WHERE point <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM vec WITH NOINDEX WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;
