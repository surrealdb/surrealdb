/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The approximate KNN operator `<|k,ef|>` names no distance, so only an HNSW or
DiskANN index search answers it. A read whose field no vector index serves --
none was defined, or it was removed -- is refused before any row is read
rather than answered with no rows. An HNSW index answers it, a brute-force
operator (`<|k,DISTANCE|>`) needs none, and `WITH NOINDEX` keeps its
contract: no index, no answer.
"""
issue = 7622

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `e`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `e`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `e`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
value = "[pts:1, pts:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:1, pts:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `other`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `e`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `vectors.*`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN operator `<|2,40|>` needs an HNSW or DiskANN index on `vectors.*`; define one, or name a distance to search without an index, as in `<|2,EUCLIDEAN|>`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[building:5, building:6]"
*/

DEFINE TABLE pts SCHEMALESS;
CREATE pts:1 SET e = [1.0, 0.0], k = 1 RETURN NONE;
CREATE pts:2 SET e = [0.0, 1.0], k = 1 RETURN NONE;
CREATE pts:3 SET e = [0.9, 0.1], k = 2 RETURN NONE;

-- No vector index: refused, with or without another conjunct, and in a block.
SELECT VALUE id FROM pts WHERE e <|2,40|> [1.0, 0.0];
SELECT VALUE id FROM pts WHERE k = 1 AND e <|2,40|> [1.0, 0.0];
{ LET $q = [1.0, 0.0]; RETURN SELECT VALUE id FROM pts WHERE e <|2,40|> $q };

-- A brute-force operator needs no index.
SELECT VALUE id FROM pts WHERE e <|2,COSINE|> [1.0, 0.0] ORDER BY id;

-- An HNSW index answers it.
DEFINE INDEX pts_e ON pts FIELDS e HNSW DIMENSION 2 DIST COSINE;
SELECT VALUE id FROM pts WHERE e <|2,40|> [1.0, 0.0] ORDER BY id;
DEFINE INDEX pts_k ON pts FIELDS k;
SELECT VALUE id FROM pts WITH INDEX pts_k WHERE k = 1 AND e <|2,40|> [1.0, 0.0] ORDER BY id;

-- An index on another field does not serve it.
SELECT VALUE id FROM pts WHERE other <|2,40|> [1.0, 0.0];

-- Removed: refused again.
REMOVE INDEX pts_e ON pts;
SELECT VALUE id FROM pts WHERE e <|2,40|> [1.0, 0.0];

-- `WITH NOINDEX` keeps its contract.
SELECT VALUE id FROM pts WITH NOINDEX WHERE e <|2,40|> [1.0, 0.0];

-- A vector index over a flattened column serves no search.
DEFINE TABLE flat SCHEMALESS;
DEFINE INDEX flat_v ON flat FIELDS vectors.* HNSW DIMENSION 2;
SELECT VALUE id FROM flat WHERE vectors.* <|2,40|> [1.0, 0.0];

-- While its build is still running, too.
DEFINE TABLE flat_building SCHEMALESS;
FOR $i IN 1..2000 { CREATE flat_building SET vectors = [[rand::float(), rand::float()]] RETURN NONE };
DEFINE INDEX flat_building_v ON flat_building FIELDS vectors.* HNSW DIMENSION 2 CONCURRENTLY;
SELECT VALUE id FROM flat_building WHERE vectors.* <|2,40|> [1.0, 0.0];

-- A field written as `type::field` is ranked by brute force while the index
-- over it is still building.
DEFINE TABLE building SCHEMALESS;
FOR $i IN 1..=3000 { CREATE type::record("building", $i) SET point = [<float> $i, 0f] RETURN NONE };
DEFINE INDEX building_p ON building FIELDS point HNSW DIMENSION 2 CONCURRENTLY;
SELECT VALUE id FROM building WHERE type::field("point") <|2,40|> [5.1f, 0f] ORDER BY id;
