/**
[env]
namespace = true
database = true
imports = ["access/record/vector_measure_param_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A vector builtin called over a field of the row and a catalog parameter whose
PERMISSIONS predicate reads the row judges the predicate against each row, as
the same call over `v ?? NONE` does: every row of `opened` admits the read, and
a row of `mixed` refuses it, which fails the statement with the parameter's
permission error. Each pair of statements answers alike.
"""

[[test.results]]
value = "[{ d: 1f, id: opened:2 }, { d: 5f, id: opened:1 }]"

[[test.results]]
value = "[{ d: 1f, id: opened:2 }, { d: 5f, id: opened:1 }]"

[[test.results]]
error = "You don't have permission to view the $q parameter"

[[test.results]]
error = "You don't have permission to view the $q parameter"

*/
SELECT id, vector::distance::euclidean(v, $q) AS d FROM opened ORDER BY d;
SELECT id, vector::distance::euclidean(v ?? NONE, $q) AS d FROM opened ORDER BY d;
SELECT id, vector::distance::euclidean(v, $q) AS d FROM mixed ORDER BY d;
SELECT id, vector::distance::euclidean(v ?? NONE, $q) AS d FROM mixed ORDER BY d;
