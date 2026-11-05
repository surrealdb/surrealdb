/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
requires = ["diskann"]
reason = "A KNN search picks its k nearest among the records every conjunct of the WHERE admits, including conjuncts the index analysis cannot read: a function call, a method, an element access"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:3, doc:4]"

[[test.results]]
value = "[doc:3, doc:4]"

[[test.results]]
value = "[doc:3, doc:4]"

[[test.results]]
value = "[doc:4, doc:5]"

[[test.results]]
value = "[dd:3, dd:4]"

[[test.results]]
value = "[bf:3, bf:4]"
*/
DEFINE INDEX hn ON doc FIELDS emb HNSW DIMENSION 2 DIST EUCLIDEAN;
DEFINE INDEX da ON dd FIELDS emb DISKANN DIMENSION 2 DIST EUCLIDEAN;
CREATE doc:1 SET emb = [0, 0], title = 'a', tags = ['p'] RETURN NONE;
CREATE doc:2 SET emb = [1, 1], title = 'a', tags = ['p'] RETURN NONE;
CREATE doc:3 SET emb = [2, 2], title = 'xa', tags = ['q'] RETURN NONE;
CREATE doc:4 SET emb = [3, 3], title = 'xb', tags = ['q'] RETURN NONE;
CREATE doc:5 SET emb = [4, 4], title = 'xc', tags = ['q'] RETURN NONE;
INSERT INTO dd (SELECT * FROM doc) RETURN NONE;
INSERT INTO bf (SELECT * FROM doc) RETURN NONE;
SELECT VALUE id FROM doc WHERE emb <|2,40|> [0, 0] AND string::contains(title, 'x') ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2,40|> [0, 0] AND title.len() > 1 ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2,40|> [0, 0] AND tags[0] = 'q' ORDER BY id;
SELECT VALUE id FROM doc WHERE emb <|2,40|> [0, 0] AND title != 'xa' AND string::contains(title, 'x') ORDER BY id;
SELECT VALUE id FROM dd WHERE emb <|2,40|> [0, 0] AND string::contains(title, 'x') ORDER BY id;
SELECT VALUE id FROM bf WHERE emb <|2, EUCLIDEAN|> [0, 0] AND string::contains(title, 'x') ORDER BY id;
