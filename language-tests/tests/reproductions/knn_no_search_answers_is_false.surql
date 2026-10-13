/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A KNN operator of a WHERE that no search answers (no index over its field) admits no row, a brute-force operator over a known vector, or over one an enclosing statement's row binds, is ranked, and one under OR is refused"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: KNN operators must appear at the top level of the WHERE clause (joined with AND); nesting `<|k, …|>` inside OR or NOT is not supported."

[[test.results]]
value = "[{ id: doc:1, near: [doc:1] }, { id: doc:2, near: [doc:2] }, { id: doc:3, near: [doc:3] }, { id: doc:4, near: [doc:4] }]"

[[test.results]]
value = "[{ id: doc:1, near: [] }, { id: doc:2, near: [] }, { id: doc:3, near: [] }, { id: doc:4, near: [] }]"

[[test.results]]
value = "[doc:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1]"
*/
CREATE doc:1 SET emb = [0, 0], b = 0 RETURN NONE;
CREATE doc:2 SET emb = [1, 1], b = 1 RETURN NONE;
CREATE doc:3 SET emb = [2, 2], b = 0 RETURN NONE;
CREATE doc:4 SET emb = [3, 3], b = 1 RETURN NONE;
SELECT VALUE id FROM doc WHERE emb <|2, EUCLIDEAN|> [0, 0] ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2, 40|> [0, 0] ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2, 40|> [0, 0] AND b = 0 ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2, EUCLIDEAN|> [0, 0] OR b = 1 ORDER BY id;
SELECT id, (SELECT VALUE id FROM doc WHERE emb <|1, EUCLIDEAN|> $parent.emb) AS near FROM doc ORDER BY id;
SELECT id, (SELECT VALUE id FROM doc WHERE emb <|1, 40|> $parent.emb) AS near FROM doc ORDER BY id;
{ LET $v = [3, 3]; RETURN SELECT VALUE id FROM doc WHERE emb <|1, EUCLIDEAN|> $v; };
{ LET $v = [3, 3]; RETURN SELECT VALUE id FROM doc WHERE emb <|1, 40|> $v; };
SELECT VALUE id FROM doc WHERE emb <|1, EUCLIDEAN|> [0, 0] AND (SELECT VALUE id FROM doc WHERE emb <|1, 40|> [3, 3]) = [] ORDER BY id;
