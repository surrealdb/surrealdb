/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	An HNSW graph holds the vectors the last write left, so a versioned
	nearest-neighbour search cannot be answered from it: a record created after
	the stamp can take a slot, one deleted after it is missing, and one that moved
	is ranked by where it went. A versioned approximate search (`<|k, ef|>`) is
	therefore the brute-force search over the records read at the stamp, and must
	equal the `WITH NOINDEX` brute-force twin in its rows and its distances.

	An approximate search whose index was defined after the stamp, or whose
	field no index covers, has no index at the stamp to rank by and is not
	rewritten.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, other: [10f], point: [10f], tag: 'a' }]"

[[test.results]]
value = "[{ id: pts:2, other: [20f], point: [20f], tag: 'a' }]"

[[test.results]]
value = "[{ id: pts:3, other: [30f], point: [30f], tag: 'b' }]"

[[test.results]]
value = "[{ id: pts:4, other: [40f], point: [40f], tag: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:0, other: [1f], point: [1f], tag: 'a' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:4, other: [2f], point: [2f], tag: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[pts:1, pts:2, pts:3]"

[[test.results]]
value = "[pts:1, pts:2, pts:3]"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[pts:1, pts:2]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 20f, id: pts:2 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 20f, id: pts:2 }]"

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 1]
            TableScan [ctx: Db] [table: pts, direction: Forward]
''''

[[test.results]]
value = "[pts:0, pts:4]"
*/

DEFINE TABLE pts SCHEMALESS;
DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN;

CREATE pts:1 SET point = [10f], other = [10f], tag = "a";
CREATE pts:2 SET point = [20f], other = [20f], tag = "a";
CREATE pts:3 SET point = [30f], other = [30f], tag = "b";
CREATE pts:4 SET point = [40f], other = [40f], tag = "a";
LET $q = [0f];
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- After the stamp: a nearer point appears, the nearest is deleted, another
-- moves next to the query, and a second index is defined over `other`.
CREATE pts:0 SET point = [1f], other = [1f], tag = "a";
DELETE pts:1;
UPDATE pts:4 SET point = [2f], other = [2f];
DEFINE INDEX hn_other ON pts FIELDS other HNSW DIMENSION 1 DIST EUCLIDEAN;

-- The approximate form and the brute-force search over the records at the
-- stamp.
SELECT VALUE id FROM pts WHERE point <|2,40|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM pts WITH NOINDEX WHERE point <|2,EUCLIDEAN|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM pts WHERE point <|3,40|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM pts WITH NOINDEX WHERE point <|3,EUCLIDEAN|> $q ORDER BY id VERSION $ts;

-- The brute-force form over a field an index of the same distance covers.
SELECT VALUE id FROM pts WHERE point <|2,EUCLIDEAN|> [0f] ORDER BY id VERSION $ts;

-- With a residual predicate: the nearest among the rows it leaves.
SELECT VALUE id FROM pts WHERE tag = "a" AND point <|2,40|> $q ORDER BY id VERSION $ts;
SELECT VALUE id FROM pts WITH NOINDEX WHERE tag = "a" AND point <|2,EUCLIDEAN|> $q ORDER BY id VERSION $ts;

-- The distances, nearest first.
SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|2,40|> $q VERSION $ts;
SELECT id, vector::distance::knn() AS d FROM pts WITH NOINDEX WHERE point <|2,EUCLIDEAN|> $q VERSION $ts;

-- The plan is the brute-force search over a scan at the stamp.
EXPLAIN SELECT VALUE id FROM pts WHERE point <|2,40|> $q VERSION $ts;

-- The same statement now, answered by the index.
SELECT VALUE id FROM pts WHERE point <|2,40|> $q ORDER BY id;
