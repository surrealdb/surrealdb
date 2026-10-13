/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Regression (#548): `knn AND <unique-indexed equality>` demoted the KNN operator off its KnnScan — the unique equality outscored the KNN candidate (1000 > 800) with no KNN-must-drive guard, so the b-tree index drove the plan and the KNN conjunct landed in a per-row residual where it is never truthy, returning zero rows. A KNN candidate now drives unconditionally; the equality is enforced as the pushed-down residual (`email` has no declared field kind, so it is not bitmap-coverable)."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'a@x.com', id: pts:1, point: [10f] }, { email: 'b@x.com', id: pts:2, point: [20f] }, { email: 'c@x.com', id: pts:3, point: [30f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: email = 'b@x.com']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: email = 'b@x.com']\n"'''

[[test.results]]
value = "[{ dist: 20f, id: pts:2 }]"
*/

DEFINE INDEX idx_email ON pts FIELDS email UNIQUE;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], email: 'a@x.com' },
	{ id: pts:2, point: [ 20f ], email: 'b@x.com' },
	{ id: pts:3, point: [ 30f ], email: 'c@x.com' }
];
LET $pt = [0f];
EXPLAIN SELECT id FROM pts
	WHERE email = 'b@x.com' AND point <|2,40|> $pt;
SELECT id, vector::distance::knn() AS dist FROM pts
	WHERE email = 'b@x.com' AND point <|2,40|> $pt
	ORDER BY dist;
