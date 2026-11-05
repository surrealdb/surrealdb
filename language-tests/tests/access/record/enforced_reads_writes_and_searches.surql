/**
[env]
namespace = true
database = true
imports = ["access/record/enforced_reads_writes_and_searches_import.surql"]
signin = """{ ns: "test", db: "test", ac: "account", user: "alice" }"""

[test]
reason = "A record user's write over a subquery, nearest-neighbour searches, a membership subquery and a LIMIT over a record range read only the rows the table's permission admits, as on main"

[[test.results]]
value = "[{ id: t:1, z: 1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ d: 0f, id: t:1 }]"

[[test.results]]
error = "Invalid query: The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. Use `<|k, EF|>` against an HNSW index (e.g. `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a brute-force KNN with an explicit distance metric."

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"
*/
UPDATE (SELECT * FROM t) SET z = 1 RETURN id, z;
SELECT id FROM t WHERE v <|2,40|> [1.0, 2.0];
SELECT id, vector::distance::knn() AS d FROM t WHERE v <|2,40|> [1.0, 2.0];
SELECT id FROM t WHERE v <|2|> [1.0, 2.0];
SELECT VALUE id FROM t WHERE id IN (SELECT VALUE id FROM t) LIMIT 1;
SELECT VALUE id FROM t:1..9 LIMIT 1;
