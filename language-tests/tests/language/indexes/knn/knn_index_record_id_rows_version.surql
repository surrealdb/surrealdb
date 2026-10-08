/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 15000

[test]
reason = "A versioned KNN whose rows need only their id still answers for the records at that version: the index describes the current state, so the search ranks the records read at the version instead, and returns the neighbours the brute-force search over them returns."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, point: [10f] }, { id: pts:2, point: [20f] }, { id: pts:3, point: [30f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:0, point: [0f] }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:0 }, { id: pts:1 }]"
*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ] },
	{ id: pts:2, point: [ 20f ] },
	{ id: pts:3, point: [ 30f ] }
];
LET $q = [0f];
SLEEP 10ms;
LET $ts = time::now();
SLEEP 10ms;
CREATE pts:0 SET point = [ 0f ];
SELECT id FROM pts WHERE point <|2,40|> $q VERSION $ts;
-- The brute-force search over the records at the version: the same answer.
SELECT id FROM pts WITH NOINDEX WHERE point <|2,EUCLIDEAN|> $q VERSION $ts;
SELECT id FROM pts WHERE point <|2,40|> $q;
