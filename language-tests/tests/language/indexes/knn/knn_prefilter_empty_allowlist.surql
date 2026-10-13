/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548), empty allow-list: coverage is per-conjunct exact, so an empty bitmap proves no record can satisfy the WHERE — the query returns no rows through the graph-free exact tier, regardless of the residual, without touching the ANN index."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', flag: true, id: pts:1, point: [10f] }, { category: 'b', flag: true, id: pts:2, point: [20f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 0}\n    Filter [ctx: Db] [predicate: category = 'zzz' AND flag = true] {rows: 0}\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: flag = true, prefilter_tier: exact] {rows: 0}\n            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'zzz'] {rows: 0}\n\nTotal rows: 0"'''

[[test.results]]
value = "[]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a', flag: true },
	{ id: pts:2, point: [ 20f ], category: 'b', flag: true }
];
LET $pt = [0f];
-- No record has category 'zzz': the allow-list is empty and the exact tier
-- yields nothing, residual notwithstanding.
EXPLAIN ANALYZE SELECT id FROM pts
	WHERE category = 'zzz' AND flag = true AND point <|2,40|> $pt;
SELECT id FROM pts
	WHERE category = 'zzz' AND flag = true AND point <|2,40|> $pt;
