/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Public issue #7544: an HNSW kNN search scores pending documents on its own and keeps their elements out of the graph results, but it must still walk through those elements. Here the index is built over two near rows and four hundred far ones on a line, then every far row is deleted and the search runs in the same transaction, so all four hundred deletes are pending. The two near rows are reachable from a far entry point only through pending elements; a search that dropped them from its frontier returned no rows."
issue = 7544

[[test.results]]
value = "[{ id: pts:1, point: [1f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [2f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ distance: 1f, id: pts:1 }, { distance: 2f, id: pts:2 }]"

[[test.results]]
value = "NONE"
*/

CREATE pts:1 SET point = [1f];
CREATE pts:2 SET point = [2f];
FOR $i IN 100..500 { CREATE type::record('pts', $i) SET point = [<float> $i]; };
DEFINE INDEX pt_hnsw ON pts FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
BEGIN;
DELETE pts WHERE point[0] >= 100f;
SELECT id, vector::distance::knn() AS distance FROM pts WHERE point <|2,40|> [0f] ORDER BY distance;
COMMIT;
