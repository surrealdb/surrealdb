/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Index scans and index-only counts must not trust DEFINE FIELD's type when an array was stored before the field was typed: the rows and counts are the table scan's"
issue = 1376

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:1, x: [3, 4] }]"

[[test.results]]
value = "[{ id: b:2, x: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }, { id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }, { id: b:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:1, x: [3, 4] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:2, x: 3 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, x: [3, 4], y: 1 }]"

[[test.results]]
value = "[{ id: d:2, x: 3, y: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: e:1, x: [3, 4], y: 1 }]"

[[test.results]]
value = "[{ id: e:2, x: 3, y: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ cat: [1, 2], id: k:1, vec: [0f, 0f] }]"

[[test.results]]
value = "[{ cat: 1, id: k:2, vec: [1f, 1f] }]"

[[test.results]]
value = "[{ cat: 1, id: k:3, vec: [2f, 2f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[k:2, k:3]"

[[test.results]]
value = "[k:2, k:3]"
*/

DEFINE INDEX i ON b FIELDS x;
CREATE b:1 SET x = [3, 4];
CREATE b:2 SET x = 3;
DEFINE FIELD x ON b TYPE int;

SELECT count() FROM b WHERE x = 3 GROUP ALL;
SELECT count() FROM b WITH NOINDEX WHERE x = 3 GROUP ALL;
SELECT count() FROM b WHERE x > 2 GROUP ALL;
SELECT count() FROM b WITH NOINDEX WHERE x > 2 GROUP ALL;
SELECT count() FROM b WHERE x < 3 GROUP ALL;
SELECT count() FROM b WITH NOINDEX WHERE x < 3 GROUP ALL;
SELECT id FROM b WHERE x = 3;
SELECT id FROM b WITH NOINDEX WHERE x = 3;
SELECT id FROM b WHERE x > 2;
SELECT id FROM b WITH NOINDEX WHERE x > 2;

DEFINE INDEX i_c ON c FIELDS x;
DEFINE FIELD x ON c TYPE array<int>;
CREATE c:1 SET x = [3, 4];
DEFINE FIELD OVERWRITE x ON c TYPE int;
CREATE c:2 SET x = 3;

SELECT count() FROM c WHERE x = 3 GROUP ALL;
SELECT count() FROM c WITH NOINDEX WHERE x = 3 GROUP ALL;

-- Bitmap fusion count (multi-index AND) with pre-existing array
DEFINE INDEX idx_d1 ON d FIELDS x;
DEFINE INDEX idx_d2 ON d FIELDS y;
CREATE d:1 SET x = [3, 4], y = 1;
CREATE d:2 SET x = 3, y = 1;
DEFINE FIELD x ON d TYPE int;
DEFINE FIELD y ON d TYPE int;

SELECT count() FROM d WHERE x = 3 AND y = 1 GROUP ALL;
SELECT count() FROM d WITH NOINDEX WHERE x = 3 AND y = 1 GROUP ALL;

-- Compound index count with pre-existing array
DEFINE INDEX idx_e ON e FIELDS x, y;
CREATE e:1 SET x = [3, 4], y = 1;
CREATE e:2 SET x = 3, y = 1;
DEFINE FIELD x ON e TYPE int;
DEFINE FIELD y ON e TYPE int;

SELECT count() FROM e WHERE x = 3 AND y = 1 GROUP ALL;
SELECT count() FROM e WITH NOINDEX WHERE x = 3 AND y = 1 GROUP ALL;

-- KNN allow-list prefilter with pre-existing array
DEFINE INDEX idx_k_cat ON k FIELDS cat;
DEFINE INDEX idx_k_vec ON k FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE k:1 SET cat = [1, 2], vec = [0.0, 0.0];
CREATE k:2 SET cat = 1, vec = [1.0, 1.0];
CREATE k:3 SET cat = 1, vec = [2.0, 2.0];
DEFINE FIELD cat ON k TYPE int;

SELECT VALUE id FROM k WHERE cat = 1 AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE cat = 1 AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
