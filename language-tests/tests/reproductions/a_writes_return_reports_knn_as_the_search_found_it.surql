/**
[env]
namespace = true
database = true

[test]
reason = "A write's RETURN clause reports a KNN operator the WHERE searches with, and vector::distance::knn() bound to it, as the search found each record before the write: the record is a neighbour and its distance is the one the search computed, even when the write moved the record's vector away from the query or out of the k nearest, and when the write deleted it. The data clause reads the same distance. The search runs against the transaction's state at the statement's start, so a vector an earlier statement of the transaction moved is searched where it was moved to. A later search ranks the records as the write left them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [0f, 0f], id: v:1 }]"

[[test.results]]
value = "[{ emb: [3f, 4f], id: v:2 }]"

[[test.results]]
value = "[{ emb: [30f, 40f], id: v:3 }]"

[[test.results]]
value = "[{ d: 0f, emb: [6f, 8f], id: v:1, k: true, now: 10f }]"

[[test.results]]
value = "[{ d: 5f, id: v:2 }]"

[[test.results]]
value = "[{ d: 5f, id: v:2, k: true, set_d: 5f }, { d: 10f, id: v:1, k: true, set_d: 10f }]"

[[test.results]]
value = "[v:3, v:1]"

[[test.results]]
value = "[{ d: 0f, id: NONE, k: true }]"

[[test.results]]
value = "[{ emb: [0f, 0f], id: t:1 }]"

[[test.results]]
value = "[{ emb: [3f, 4f], id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 1f, id: t:2, k: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [0f, 0f], id: t:1 }, { emb: [9f, 9f], id: t:2 }]"
*/
DEFINE INDEX ix ON v FIELDS emb HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE v:1 SET emb = [0f, 0f];
CREATE v:2 SET emb = [3f, 4f];
CREATE v:3 SET emb = [30f, 40f];
UPDATE v SET emb = [6f, 8f] WHERE emb <|1,40|> [0f, 0f] RETURN id, emb, vector::distance::knn() AS d, emb <|1,40|> [0f, 0f] AS k, vector::distance::euclidean(emb, [0f, 0f]) AS now;
SELECT id, vector::distance::knn() AS d FROM v WHERE emb <|1,40|> [0f, 0f];
UPDATE v SET emb = IF id = v:1 THEN [100f, 100f] ELSE [200f, 200f] END, set_d = vector::distance::knn() WHERE emb <|2,40|> [0f, 0f] RETURN id, set_d, vector::distance::knn() AS d, emb <|2,40|> [0f, 0f] AS k;
SELECT VALUE id FROM v WHERE emb <|2,40|> [0f, 0f];
DELETE v WHERE emb <|1,40|> [30f, 40f] RETURN id, vector::distance::knn() AS d, emb <|1,40|> [30f, 40f] AS k;
CREATE t:1 SET emb = [0f, 0f];
CREATE t:2 SET emb = [3f, 4f];
DEFINE INDEX ix ON t FIELDS emb HNSW DIMENSION 2 DIST EUCLIDEAN;
BEGIN;
UPDATE t:2 SET emb = [1f, 0f] RETURN NONE;
UPDATE t SET emb = [9f, 9f] WHERE emb <|1,40|> [1f, 1f] RETURN id, vector::distance::knn() AS d, emb <|1,40|> [1f, 1f] AS k;
COMMIT;
SELECT id, emb FROM t ORDER BY id;
