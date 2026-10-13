/**
[env]
namespace = true
database = true

[test]
reason = """
A brute-force KNN (`<|k,DIST|>`) skips every record whose vector cannot be
compared with the query vector: a query vector of another dimension ranks no
record, and in a table holding vectors of several dimensions only the records
of the query's dimension are ranked. An empty query vector has no dimension a
record's could share, and is refused, under every distance and as a
parameter's value. A distance an ANN index does not serve stays brute force.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
value = "[{ id: bpts:9, point: [1.5f, 0f, 0f] }]"

[[test.results]]
value = "[bpts:1, bpts:2]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"
*/
DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..4 {
	CREATE type::record('bpts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f];
};
-- An empty query vector.
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [];
-- A query vector of another dimension than every record.
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [1f, 2f, 3f];
-- Another distance.
SELECT VALUE id FROM bpts WHERE point <|1,COSINE|> [];
-- The empty query vector as a parameter.
LET $q = [];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q;
-- Records of several dimensions: the 3-dimensional one, nearest by its first
-- two components, is not ranked.
CREATE bpts:9 SET point = [1.5f, 0f, 0f];
SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> [1.5f, 0f] ORDER BY id;
-- A distance the table's HNSW index does not serve stays brute force.
SELECT VALUE id FROM pts WHERE point <|1,MANHATTAN|> [];
