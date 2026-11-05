/**
[env]
namespace = true
database = true
imports = ["reproductions/knn_operator_in_write_where_record_user_import.surql"]
signin = """{ ns: "test", db: "test", ac: "account", user: "alice" }"""

[test]
reason = "As record user alice, a brute-force KNN operator in the WHERE of an UPDATE or DELETE is refused. The nested SELECT form selects the nearest rows alice may select, never a hidden row even when the hidden row is nearer to the query vector. An HNSW-indexed write is answered by its index."

[[test.results]]
value = "[t:1]"

[[test.results]]
error = "Invalid query: A KNN operator in the WHERE of an UPDATE or DELETE needs a vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`."

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t_h:1]"

[[test.results]]
value = "[{ id: t:1, owner: user:alice, v: [1f, 0f, 0f], w: 1 }]"

[[test.results]]
value = "[{ id: t:3, owner: user:alice, v: [5f, 5f, 5f] }]"

[[test.results]]
value = "[t_h:1]"
*/
SELECT VALUE id FROM t WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0];
UPDATE t SET w = 1 WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0] RETURN VALUE id;
UPDATE (SELECT id FROM t WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0]) SET w = 1 RETURN VALUE id;
SELECT VALUE id FROM t_h WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0];
DELETE (SELECT id FROM t WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0]) RETURN BEFORE;
DELETE (SELECT id FROM t WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0]) RETURN BEFORE;
SELECT VALUE id FROM t_h WHERE v <|1,EUCLIDEAN|> [0.2, 0.0, 0.0];
