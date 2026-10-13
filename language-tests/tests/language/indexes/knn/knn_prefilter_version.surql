/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
planner-strategy = ["all-ro"]
timeout = 15000

[test]
reason = "Pre-filtered vector search (#548) is never attached to VERSION queries: doc-ID mappings and index entry bitmaps are not time-travel-aware, so a versioned KNN keeps the pre-existing in-traversal residual plan while the unversioned twin gets the bitmap prefilter."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', id: pts:1, point: [10f] }, { category: 'b', id: pts:2, point: [20f] }, { category: 'a', id: pts:3, point: [30f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"VersionScope [ctx: Db] [version: $ts]\n    SelectProject [ctx: Db] [projections: id]\n        Filter [ctx: Db] [predicate: category = 'a']\n            KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: category = 'a']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'a']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1]\n            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a']\n"'''
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a' },
	{ id: pts:2, point: [ 20f ], category: 'b' },
	{ id: pts:3, point: [ 30f ], category: 'a' }
];
LET $pt = [0f];
LET $ts = time::now();
-- VERSION keeps the pre-prefilter plan: the whole residual is pushed into
-- the traversal and no bitmap child appears.
EXPLAIN SELECT id FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt VERSION $ts;
-- The unversioned twin gets the prefilter.
EXPLAIN SELECT id FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt;
