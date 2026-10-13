/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548): a fully index-coverable WHERE conjunct becomes an allow-list bitmap prefilter under the KnnScan — the plan shows the bitmap child and no pushed-down predicate, EXPLAIN ANALYZE reports the exact tier taken, and results match the filtered ground truth with zero record fetches inside the search."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', id: pts:1, point: [10f] }, { category: 'b', id: pts:2, point: [20f] }, { category: 'a', id: pts:3, point: [30f] }, { category: 'b', id: pts:4, point: [40f] }, { category: 'a', id: pts:5, point: [50f] }, { category: 'b', id: pts:6, point: [60f] }, { category: 'a', id: pts:7, point: [70f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, category, distance]\n    SortByKey [ctx: Db] [sort_keys: distance ASC]\n        Compute [ctx: Db] [fields: distance = vector::distance::knn(...)]\n            Filter [ctx: Db] [predicate: category = 'a']\n                KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1]\n                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, category, distance] {rows: 2}\n    SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2}\n        Compute [ctx: Db] [fields: distance = vector::distance::knn(...)] {rows: 2}\n            Filter [ctx: Db] [predicate: category = 'a'] {rows: 2}\n                KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, prefilter_tier: exact] {rows: 2}\n                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a'] {rows: 4}\n\nTotal rows: 2"'''

[[test.results]]
value = "[{ category: 'a', distance: 6f, id: pts:5 }, { category: 'a', distance: 14f, id: pts:3 }]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a' },
	{ id: pts:2, point: [ 20f ], category: 'b' },
	{ id: pts:3, point: [ 30f ], category: 'a' },
	{ id: pts:4, point: [ 40f ], category: 'b' },
	{ id: pts:5, point: [ 50f ], category: 'a' },
	{ id: pts:6, point: [ 60f ], category: 'b' },
	{ id: pts:7, point: [ 70f ], category: 'a' }
];
LET $pt = [44f];
-- Plain EXPLAIN: the covered conjunct renders as a bitmap child of the
-- KnnScan; no predicate is pushed into the traversal; the tier is a runtime
-- decision and does not appear.
EXPLAIN SELECT id, category, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt
	ORDER BY distance;
-- EXPLAIN ANALYZE: four allow-list members (<= the exact threshold) take the
-- graph-free exact tier; the bitmap child reports its cardinality.
EXPLAIN ANALYZE SELECT id, category, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt
	ORDER BY distance;
-- Results match the filtered brute-force ground truth.
SELECT id, category, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt
	ORDER BY distance;
