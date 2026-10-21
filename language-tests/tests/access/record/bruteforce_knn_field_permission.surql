/**
[env]
namespace = true
database = true
imports = ["access/record/bruteforce_knn_field_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A brute-force KNN (`<|k,DIST|>`) ranks a record by the vector the caller can
see. A field-level SELECT permission that hides the vector leaves the record
unranked: `fp:2`, whose vector sits nearest the query, is someone else's. A
COMPUTED vector is ranked by the value it computes, never by bytes stored
under its name: `cp:1` stored `[0f]` before the field was defined, and
computes `[3f]`. The same holds against a parameter's vector, and for a
vector measure over the field, which answers what the same call over
`point ?? NONE` answers: over `fp`, the hidden vector of `fp:2` is `NONE` to
the measure, which raises the call's error rather than measure the bytes
stored for it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 1f, id: fp:1 }, { d: 2f, id: fp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: fp:1 }, { d: 2f, id: fp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: cp:2 }, { d: 2f, id: cp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: cp:2 }, { d: 2f, id: cp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: fp:1 }, { d: 2f, id: fp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: fp:1 }, { d: 2f, id: fp:3 }]"

[[test.results]]
value = "[{ d: 1f, id: cp:2 }, { d: 2f, id: cp:3 }, { d: 3f, id: cp:1 }]"

[[test.results]]
value = "[{ d: 1f, id: cp:2 }, { d: 2f, id: cp:3 }, { d: 3f, id: cp:1 }]"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

*/

LET $q = [0f];
SELECT id, vector::distance::knn() AS d FROM fp WHERE point <|5,EUCLIDEAN|> [0f] ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM fp WHERE point <|5,EUCLIDEAN|> $q ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM cp WHERE point <|2,EUCLIDEAN|> [0f] ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM cp WHERE point <|2,EUCLIDEAN|> $q ORDER BY d;
SELECT id, vector::distance::euclidean(point, $q) AS d FROM fp WHERE point != NONE ORDER BY d;
SELECT id, vector::distance::euclidean(point ?? NONE, $q) AS d FROM fp WHERE point != NONE ORDER BY d;
SELECT id, vector::distance::euclidean(point, [0f]) AS d FROM cp ORDER BY d;
SELECT id, vector::distance::euclidean(point ?? NONE, [0f]) AS d FROM cp ORDER BY d;
SELECT id, vector::distance::euclidean(point, $q) AS d FROM fp ORDER BY id;
SELECT id, vector::distance::euclidean(point ?? NONE, $q) AS d FROM fp ORDER BY id;
