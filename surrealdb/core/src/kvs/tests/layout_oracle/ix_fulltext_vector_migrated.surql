-- A full-text index and a vector index defined over migrated fields before the job drains them. The
-- text migration trims and lowercases, so the index holds the migrated terms, and a vector of the
-- wrong length migrates to the zero vector and is indexed as one. REBUILD INDEX after the drain must
-- keep the same answers. Query vectors are chosen so no two candidates are equidistant.
DEFINE ANALYZER an2 TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE ft2 SCHEMAFULL;
DEFINE FIELD body ON ft2 TYPE string;
DEFINE FIELD vec ON ft2 TYPE array;
CREATE ft2:1 SET body = '  HELLO World  ', vec = ['1', '0'] RETURN NONE;
CREATE ft2:2 SET body = 'Goodbye World', vec = ['0', '1'] RETURN NONE;
CREATE ft2:3 SET body = 'plain', vec = ['1', '1', '1'] RETURN NONE;
CREATE ft2:4 SET body = 'hello again', vec = ['3', '3'] RETURN NONE;
-- @upgrade
ALTER FIELD body ON ft2 TYPE string MIGRATE EXISTING ROWS AS body_norm USING string::lowercase(string::trim($value)) FALLBACK '';
DEFINE FIELD OVERWRITE vec ON ft2 TYPE array<float, 2> MIGRATE EXISTING ROWS AS vec_f USING array::map($value, |$x| <float>$x) FALLBACK [0f, 0f];
DEFINE INDEX ftb ON ft2 FIELDS body FULLTEXT ANALYZER an2 BM25;
DEFINE INDEX hv ON ft2 FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
SELECT VALUE id FROM ft2 WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM ft2 WHERE body @@ 'hello' ORDER BY id EXPLAIN;
SELECT VALUE id FROM ft2 WHERE body @@ 'world' ORDER BY id;
SELECT VALUE body FROM ft2 ORDER BY id;
SELECT VALUE vec FROM ft2 ORDER BY id;
SELECT VALUE id FROM ft2 WHERE vec <|2,40|> [1f, 0f];
SELECT VALUE id FROM ft2 WHERE vec <|2,40|> [1f, 0f] EXPLAIN;
-- @check
UPDATE ft2:2 SET body = 'Hello Earth' RETURN NONE;
SELECT VALUE id FROM ft2 WHERE body @@ 'goodbye' ORDER BY id;
SELECT VALUE id FROM ft2 WHERE body @@ 'hello' ORDER BY id;
DELETE ft2:1;
SELECT VALUE id FROM ft2 WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM ft2 WHERE vec <|1,40|> [0f, 1f];
-- @drain
REBUILD INDEX ftb ON ft2;
REBUILD INDEX hv ON ft2;
SELECT VALUE id FROM ft2 WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM ft2 WHERE vec <|2,40|> [0.1f, 0.2f];
-- @check
