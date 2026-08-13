/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548), partial coverage: the exactly index-coverable conjunct feeds the allow-list bitmap while the non-indexable one stays as the true residual — the KnnScan predicate shows only the residual, and both predicates are enforced in the results."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', flag: true, id: pts:1, point: [10f] }, { category: 'b', flag: true, id: pts:2, point: [20f] }, { category: 'a', flag: false, id: pts:3, point: [30f] }, { category: 'b', flag: false, id: pts:4, point: [40f] }, { category: 'a', flag: true, id: pts:5, point: [50f] }, { category: 'b', flag: true, id: pts:6, point: [60f] }, { category: 'a', flag: true, id: pts:7, point: [70f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, distance] {rows: 2}\n    SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 2}\n        Compute [ctx: Db] [fields: distance = vector::distance::knn(...)] {rows: 2}\n            Filter [ctx: Db] [predicate: category = 'a' AND flag = true] {rows: 2}\n                KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: flag = true, prefilter_tier: exact] {rows: 2}\n                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a'] {rows: 4}\n\nTotal rows: 2"'''

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 50f, id: pts:5 }]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a', flag: true },
	{ id: pts:2, point: [ 20f ], category: 'b', flag: true },
	{ id: pts:3, point: [ 30f ], category: 'a', flag: false },
	{ id: pts:4, point: [ 40f ], category: 'b', flag: false },
	{ id: pts:5, point: [ 50f ], category: 'a', flag: true },
	{ id: pts:6, point: [ 60f ], category: 'b', flag: true },
	{ id: pts:7, point: [ 70f ], category: 'a', flag: true }
];
LET $pt = [0f];
-- `category` is covered by the bitmap; `flag` (unindexed) is the true
-- residual pushed into the search.
EXPLAIN ANALYZE SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND flag = true AND point <|2,40|> $pt
	ORDER BY distance;
-- Nearest category-'a' AND flagged: pts:1 (10) and pts:5 (50) — pts:3 is
-- closer than pts:5 but unflagged and must not consume a top-K slot.
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND flag = true AND point <|2,40|> $pt
	ORDER BY distance;
