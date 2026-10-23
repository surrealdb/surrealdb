/**
[env]
namespace = true
database = true

[test]
reason = "A brute-force KNN operator in the WHERE of an UPDATE or DELETE is refused, since no vector index answers it. The nested SELECT form selects the k nearest rows that satisfy the other conjuncts, as the SELECT does. An HNSW-indexed write is answered by its index. UPSERT with a KNN operator is refused."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: A KNN operator in the WHERE of an UPDATE or DELETE needs a vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`."

[[test.results]]
error = "Invalid query: A KNN operator in the WHERE of an UPDATE or DELETE needs a vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: A KNN operator in the WHERE of an UPDATE or DELETE needs a vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`."

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: A KNN operator in the WHERE of an UPDATE or DELETE needs a vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`."

[[test.results]]
value = "[bu_u:1, bu_u:2, bu_u:3, bu_u:4]"

[[test.results]]
value = "[bu_u:1]"

[[test.results]]
value = "[bu_u:1]"

[[test.results]]
value = "[bu_u:4]"

[[test.results]]
value = "[bu_u:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[bu_u:1, bu_u:2, bu_u:3, bu_u:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[bu_u:1, bu_u:2, bu_u:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[bu_h:1]"

[[test.results]]
value = "[{ id: bu_h:3, v: [5, 5, 5] }]"

[[test.results]]
value = "[bu_h:1, bu_h:2, bu_h:4]"

[[test.results]]
error = "Invalid query: A KNN operator is not supported in the WHERE of an UPSERT; use UPDATE, or UPSERT the ids a SELECT returns."
*/

-- Brute force on a table with no vector index: bu_u:1 [1,0,0] g=1, bu_u:2 [0,1,0] g=2,
-- bu_u:3 [5,5,5] g=1, bu_u:4 [1,1,0] g=2.
DEFINE TABLE bu_u;
INSERT INTO bu_u [{id:bu_u:1,v:[1,0,0],g:1},{id:bu_u:2,v:[0,1,0],g:2},{id:bu_u:3,v:[5,5,5],g:1},{id:bu_u:4,v:[1,1,0],g:2}] RETURN NONE;
UPDATE bu_u SET w = 1 WHERE v <|1,EUCLIDEAN|> [1,0,0] RETURN VALUE id;
UPDATE bu_u SET w = 1 WHERE v <|1,EUCLIDEAN|> [1,0,0] AND g = 2 RETURN VALUE id;
LET $q = [1,0,0];
UPDATE bu_u SET w = 1 WHERE v <|1,EUCLIDEAN|> $q RETURN VALUE id;
SELECT VALUE id FROM bu_u WHERE w = 1 ORDER BY id;
DELETE bu_u WHERE v <|1,EUCLIDEAN|> [9,9,9] RETURN NONE;
SELECT VALUE id FROM bu_u ORDER BY id;

-- The nested SELECT form: the k nearest rows, with a conjunct, and k larger than the row count.
UPDATE (SELECT id FROM bu_u WHERE v <|1,EUCLIDEAN|> [1,0,0]) SET w = 1 RETURN VALUE id;
SELECT VALUE id FROM bu_u WHERE w = 1;
UPDATE (SELECT id FROM bu_u WHERE v <|1,EUCLIDEAN|> [1,0,0] AND g = 2) SET w = 2 RETURN VALUE id;
SELECT VALUE id FROM bu_u WHERE w = 2;
UPDATE (SELECT id FROM bu_u WHERE v <|10,EUCLIDEAN|> [1,0,0]) SET w = 3 RETURN NONE;
SELECT VALUE id FROM bu_u WHERE w = 3 ORDER BY id;
DELETE (SELECT id FROM bu_u WHERE v <|1,EUCLIDEAN|> [9,9,9]) RETURN NONE;
SELECT VALUE id FROM bu_u ORDER BY id;

-- HNSW control: the same write is answered by the index.
DEFINE TABLE bu_h;
INSERT INTO bu_h [{id:bu_h:1,v:[1,0,0]},{id:bu_h:2,v:[0,1,0]},{id:bu_h:3,v:[5,5,5]},{id:bu_h:4,v:[1,1,0]}] RETURN NONE;
DEFINE INDEX bu_h_v ON bu_h FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN;
UPDATE bu_h SET w = 1 WHERE v <|1,EUCLIDEAN|> [1,0,0] RETURN VALUE id;
DELETE bu_h WHERE v <|1,EUCLIDEAN|> [9,9,9] RETURN BEFORE;
SELECT VALUE id FROM bu_h ORDER BY id;

-- UPSERT with a KNN operator is refused.
UPSERT bu_u SET w = 9 WHERE v <|1,EUCLIDEAN|> [5,0,0] RETURN VALUE id;
