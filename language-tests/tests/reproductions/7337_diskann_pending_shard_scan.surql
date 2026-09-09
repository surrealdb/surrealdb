/**
[env]

[test]
reason = "Public issue #7337: DiskANN held pending record updates in a single unsharded `!dr` range, so KNN lookups scanned the whole pending set on every query while a write backlog existed. Pending records are now stored under a shard-prefixed `!dw` key and lookup merges per-shard. This exercises the end-to-end pending path: records are inserted (landing in the pending set across several shards) and a KNN query must still return the correct nearest neighbours before background compaction folds them into the graph."
issue = 7337

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, point: [10f] }, { id: pts:2, point: [20f] }, { id: pts:3, point: [30f] }, { id: pts:4, point: [40f] }, { id: pts:5, point: [50f] }, { id: pts:6, point: [60f] }, { id: pts:7, point: [70f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 4f, id: pts:4 }, { distance: 6f, id: pts:5 }, { distance: 14f, id: pts:3 }]"
*/

DEFINE INDEX da_pt ON pts FIELDS point DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ] },
	{ id: pts:2, point: [ 20f ] },
	{ id: pts:3, point: [ 30f ] },
	{ id: pts:4, point: [ 40f ] },
	{ id: pts:5, point: [ 50f ] },
	{ id: pts:6, point: [ 60f ] },
	{ id: pts:7, point: [ 70f ] }
];
LET $pt = [44f];
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE point <|3,40|> $pt
	ORDER BY distance;
