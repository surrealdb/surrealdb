/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548), duplicate vectors: identical vectors share one graph element carrying several doc-IDs. Element admission needs only one allowed doc, but result materialization gates per doc — a sibling doc outside the allow-list must neither surface nor consume a top-K slot. Covered for both HNSW and DiskANN (whose admission is per-doc by construction)."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', id: pts:1, point: [10f] }, { category: 'b', id: pts:2, point: [10f] }, { category: 'a', id: pts:3, point: [30f] }, { category: 'a', id: pts:4, point: [40f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 30f, id: pts:3 }, { distance: 40f, id: pts:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dacategory: 'a', id: dpts:1, point: [10f] }, { dacategory: 'b', id: dpts:2, point: [10f] }, { dacategory: 'a', id: dpts:3, point: [30f] }, { dacategory: 'a', id: dpts:4, point: [40f] }]"

[[test.results]]
value = "[{ distance: 10f, id: dpts:1 }, { distance: 30f, id: dpts:3 }, { distance: 40f, id: dpts:4 }]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
-- pts:1 and pts:2 share one vector (one graph element, two doc-IDs) but
-- only pts:1 is category 'a'.
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a' },
	{ id: pts:2, point: [ 10f ], category: 'b' },
	{ id: pts:3, point: [ 30f ], category: 'a' },
	{ id: pts:4, point: [ 40f ], category: 'a' }
];
LET $pt = [0f];
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|3,40|> $pt
	ORDER BY distance;

DEFINE FIELD dacategory ON dpts TYPE string;
DEFINE INDEX idx_dacategory ON dpts FIELDS dacategory;
DEFINE INDEX da_pt1 ON dpts FIELDS point DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO dpts [
	{ id: dpts:1, point: [ 10f ], dacategory: 'a' },
	{ id: dpts:2, point: [ 10f ], dacategory: 'b' },
	{ id: dpts:3, point: [ 30f ], dacategory: 'a' },
	{ id: dpts:4, point: [ 40f ], dacategory: 'a' }
];
SELECT id, vector::distance::knn() AS distance FROM dpts
	WHERE dacategory = 'a' AND point <|3,40|> $pt
	ORDER BY distance;
