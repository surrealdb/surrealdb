/**
[env]
planner-strategy = ["all-ro"]
timeout = 120000

[test]
reason = "Pre-filtered vector search (#548), graph tier: an allow-list larger than the exact threshold (default 2000) keeps the ANN traversal, gating candidate admission on bitmap membership with boosted ef. 2500 of 3000 records match the covered conjunct; the nearest matching points are returned and EXPLAIN ANALYZE reports the graph tier."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, distance] {rows: 3}\n    SortByKey [ctx: Db] [sort_keys: distance ASC] {rows: 3}\n        Compute [ctx: Db] [fields: distance = vector::distance::knn(...)] {rows: 3}\n            Filter [ctx: Db] [predicate: category = 'a'] {rows: 3}\n                KnnScan [ctx: Db] [index: hn_pt1, k: 3, ef: 40, dimension: 1, prefilter_tier: graph] {rows: 3}\n                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a'] {rows: 2500}\n\nTotal rows: 3"'''

[[test.results]]
value = "[{ distance: 0f, id: pts:0 }, { distance: 1f, id: pts:1 }, { distance: 2f, id: pts:2 }]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
-- 3000 points on a line; five of every six are category 'a' (2500 > the
-- 2000 exact-tier threshold).
FOR $i IN 0..3000 {
	CREATE type::record('pts', $i) SET
		point = [ <float> $i ],
		category = IF $i % 6 < 5 { 'a' } ELSE { 'b' };
};
LET $pt = [0f];
EXPLAIN ANALYZE SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|3,40|> $pt
	ORDER BY distance;
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE category = 'a' AND point <|3,40|> $pt
	ORDER BY distance;
