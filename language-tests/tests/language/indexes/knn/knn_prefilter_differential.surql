/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548) differential battery: each filtered ANN query (`<|k,ef|>`, planned with an allow-list prefilter where conjuncts are exactly coverable) is paired with its brute-force ground-truth twin (`<|k,EUCLIDEAN|>`, a KnnTopK full scan + filter that never touches the prefilter machinery) and must return identical rows — across equality, range, OR, negation, multi-conjunct, partial-coverage, unsatisfiable shapes, and rows missing the residual column. The legacy compute-only planner is not used as the oracle: it independently mis-plans KNN with an indexed sibling conjunct (the demotion bug fixed for the streaming planner by #548)."
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
value = "[{ category: 1, extra: 'x', flag: true, id: pts:1, point: [10f] }, { category: 2, extra: 'y', flag: false, id: pts:2, point: [20f] }, { category: 3, extra: 'x', flag: true, id: pts:3, point: [30f] }, { category: 1, extra: 'y', flag: false, id: pts:4, point: [40f] }, { category: 2, extra: 'x', flag: true, id: pts:5, point: [50f] }, { category: 3, extra: 'y', flag: false, id: pts:6, point: [60f] }, { category: 1, extra: 'x', flag: true, id: pts:7, point: [70f] }, { category: 2, extra: 'y', flag: false, id: pts:8, point: [80f] }, { category: 3, extra: 'x', flag: true, id: pts:9, point: [90f] }, { category: 1, extra: 'y', flag: false, id: pts:10, point: [100f] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 40f, id: pts:4 }, { d: 70f, id: pts:7 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 40f, id: pts:4 }, { d: 70f, id: pts:7 }]"

[[test.results]]
value = "[{ d: 20f, id: pts:2 }, { d: 50f, id: pts:5 }, { d: 80f, id: pts:8 }]"

[[test.results]]
value = "[{ d: 20f, id: pts:2 }, { d: 50f, id: pts:5 }, { d: 80f, id: pts:8 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 30f, id: pts:3 }, { d: 40f, id: pts:4 }, { d: 60f, id: pts:6 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 30f, id: pts:3 }, { d: 40f, id: pts:4 }, { d: 60f, id: pts:6 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }, { d: 110f, id: pts:11 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }, { d: 110f, id: pts:11 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }, { d: 110f, id: pts:11 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }, { d: 110f, id: pts:11 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }]"

[[test.results]]
value = "[{ d: 10f, id: pts:1 }, { d: 70f, id: pts:7 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE FIELD category ON pts TYPE int;
DEFINE FIELD flag ON pts TYPE bool;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX idx_flag ON pts FIELDS flag;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1,  point: [ 10f ],  category: 1, flag: true,  extra: 'x' },
	{ id: pts:2,  point: [ 20f ],  category: 2, flag: false, extra: 'y' },
	{ id: pts:3,  point: [ 30f ],  category: 3, flag: true,  extra: 'x' },
	{ id: pts:4,  point: [ 40f ],  category: 1, flag: false, extra: 'y' },
	{ id: pts:5,  point: [ 50f ],  category: 2, flag: true,  extra: 'x' },
	{ id: pts:6,  point: [ 60f ],  category: 3, flag: false, extra: 'y' },
	{ id: pts:7,  point: [ 70f ],  category: 1, flag: true,  extra: 'x' },
	{ id: pts:8,  point: [ 80f ],  category: 2, flag: false, extra: 'y' },
	{ id: pts:9,  point: [ 90f ],  category: 3, flag: true,  extra: 'x' },
	{ id: pts:10, point: [ 100f ], category: 1, flag: false, extra: 'y' }
];
-- Rows missing the schemaless residual column `extra` entirely (covered
-- columns are typed and therefore always present; missing-field semantics
-- only arise on the residual path).
CREATE pts:11 SET point = [ 110f ], category = 1, flag = true RETURN NONE;
CREATE pts:12 SET point = [ 120f ], category = 2, flag = false RETURN NONE;
LET $pt = [0f];
-- Equality.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
-- Two half-bounded ranges intersecting on the covered column.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category > 1 AND category < 3 AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category > 1 AND category < 3 AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
-- OR of coverable equalities.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE (category = 1 OR category = 3) AND point <|4,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE (category = 1 OR category = 3) AND point <|4,EUCLIDEAN|> $pt ORDER BY d;
-- Negation stays residual.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND !(flag = false) AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND !(flag = false) AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
-- Two covered conjuncts intersected.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND flag = true AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND flag = true AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
-- Partial coverage: `extra` is unindexed and missing on pts:11.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND extra = 'x' AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 1 AND extra = 'x' AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
-- Unsatisfiable covered conjunct.
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 99 AND point <|3,40|> $pt ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM pts
	WHERE category = 99 AND point <|3,EUCLIDEAN|> $pt ORDER BY d;
