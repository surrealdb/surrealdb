/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
requires = ["diskann"]
reason = "Writes keep every kind of index of a table in line with its records: B-tree, unique, count with and without a guard, full-text, HNSW, DiskANN and spatial"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', code: 1, emb: [1, 2, 3], emb2: [1, 2, 3], id: t:1, loc: (0f, 51.5f), name: 'a', tag: 'hot' }]"

[[test.results]]
value = "[{ body: 'goodbye world', code: 2, emb: [4, 5, 6], emb2: [4, 5, 6], id: t:2, loc: (2.3f, 48.8f), name: 'b', tag: 'cold' }]"

[[test.results]]
value = "[{ body: 'hello again', code: 3, emb: [7, 8, 9], emb2: [7, 8, 9], id: t:3, loc: (139.7f, 35.7f), name: 'a', tag: 'hot' }]"

[[test.results]]
error = "Database index `by_code` already contains 2, with record `t:2`"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[t:1, t:3]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:1, t:3]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[{ body: 'hello there', code: 2, emb: [0, 0, 0], emb2: [0, 0, 0], id: t:2, loc: (0f, 51.5f), name: 'a', tag: 'hot' }]"

[[test.results]]
value = "[{ body: 'hello world', code: 10, emb: [1, 2, 3], emb2: [1, 2, 3], id: t:1, loc: (0f, 51.5f), name: 'a', tag: 'cold' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE TABLE t;
DEFINE INDEX by_name ON t FIELDS name;
DEFINE INDEX by_code ON t FIELDS code UNIQUE;
DEFINE INDEX total ON t COUNT;
DEFINE INDEX hot ON t COUNT WHERE tag = 'hot';
DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX emb_hnsw ON t FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
DEFINE INDEX emb_ann ON t FIELDS emb2 DISKANN DIMENSION 3 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX loc_spatial ON t FIELDS loc SPATIAL;
CREATE t:1 SET name = 'a', code = 1, tag = 'hot', body = 'hello world', emb = [1, 2, 3], emb2 = [1, 2, 3], loc = (0.0, 51.5);
CREATE t:2 SET name = 'b', code = 2, tag = 'cold', body = 'goodbye world', emb = [4, 5, 6], emb2 = [4, 5, 6], loc = (2.3, 48.8);
CREATE t:3 SET name = 'a', code = 3, tag = 'hot', body = 'hello again', emb = [7, 8, 9], emb2 = [7, 8, 9], loc = (139.7, 35.7);
CREATE t:4 SET code = 2;
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t WHERE tag = 'hot' GROUP ALL;
SELECT VALUE id FROM t WHERE name = 'a' ORDER BY id;
SELECT VALUE id FROM t WHERE code = 3;
SELECT VALUE id FROM t WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM t WHERE emb <|1,40|> [4, 5, 6];
SELECT VALUE id FROM t WHERE emb2 <|1,40|> [7, 8, 9];
SELECT VALUE id FROM t WHERE geo::distance(loc, (0.0, 51.5)) < 1000.0;
UPDATE t:2 SET tag = 'hot', name = 'a', body = 'hello there', emb = [0, 0, 0], emb2 = [0, 0, 0], loc = (0.0, 51.5);
UPDATE t:1 SET tag = 'cold', code = 10;
DELETE t:3;
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t WHERE tag = 'hot' GROUP ALL;
SELECT VALUE id FROM t WHERE name = 'a' ORDER BY id;
SELECT VALUE id FROM t WHERE code = 10;
SELECT VALUE id FROM t WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM t WHERE emb <|1,40|> [0, 0, 0];
SELECT VALUE id FROM t WHERE emb2 <|1,40|> [0, 0, 0];
SELECT VALUE id FROM t WHERE geo::distance(loc, (0.0, 51.5)) < 1000.0 ORDER BY id;
DELETE t;
SELECT count() FROM t GROUP ALL;
SELECT VALUE id FROM t WHERE body @@ 'hello';
SELECT VALUE id FROM t WHERE geo::distance(loc, (0.0, 51.5)) < 1000.0;
