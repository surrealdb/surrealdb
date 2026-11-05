/**
[env]
namespace = true
database = true

[test]
reason = "A KNN operator in the WHERE of an UPDATE or DELETE is answered by the search it names, as in a SELECT: an HNSW search and a brute-force search each reach their nearest records, and the distance the search found is readable in the data clause."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, point: [1, 1] }]"

[[test.results]]
value = "[{ id: pts:2, point: [2, 2] }]"

[[test.results]]
value = "[{ id: pts:3, point: [10, 10] }]"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[pts:3]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ bf: NONE, id: pts:2, near: true }, { bf: true, id: pts:3, near: NONE }]"

[[test.results]]
error = "Invalid query: A KNN operator is not supported in the WHERE of an UPSERT; use UPDATE, or UPSERT the ids a SELECT returns."

[[test.results]]
value = "[0f]"

[[test.results]]
value = "[{ id: pts:2, up: NONE }, { id: pts:3, up: NONE }]"

*/

DEFINE INDEX hnsw ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE pts:1 SET point = [1, 1];
CREATE pts:2 SET point = [2, 2];
CREATE pts:3 SET point = [10, 10];
UPDATE pts SET near = true WHERE point <|2,40|> [0, 0] RETURN VALUE id;
UPDATE pts SET bf = true WHERE point <|1,EUCLIDEAN|> [9, 9] RETURN VALUE id;
DELETE pts WHERE point <|1,40|> [0, 0] RETURN VALUE id;
SELECT id, near, bf FROM pts ORDER BY id;
UPSERT pts SET up = true WHERE point <|1,40|> [10, 10] RETURN VALUE id;
UPDATE pts SET d = vector::distance::knn() WHERE point <|1,40|> [10, 10] RETURN VALUE d;
SELECT id, up FROM pts ORDER BY id;
