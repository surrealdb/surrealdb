/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548) plan-shape gates: an OR of exactly-coverable equalities joins the allow-list as a BitmapOr; a negated conjunct always stays residual (no table-universe bitmap); a WITH INDEX hint pins the plan and disables the prefilter; a WITH INDEX hint naming a different index is overridden (with a warning) because a KNN operator can only be computed by its KnnScan; an indexed column without a declared array-free field kind is not provably exact and stays residual."
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
value = "[{ category: 'a', id: pts:1, point: [10f], rawcat: 'a' }, { category: 'b', id: pts:2, point: [20f], rawcat: 'b' }, { category: 'c', id: pts:3, point: [30f], rawcat: 'c' }, { category: 'a', id: pts:4, point: [40f], rawcat: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'a' OR category = 'b']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1]\n            BitmapOr [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a']\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'b']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'a' AND ! category = 'b']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: !(category = 'b')]\n            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'a']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: category = 'a']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'a']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: category = 'a']\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: rawcat = 'a']\n        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1, predicate: rawcat = 'a']\n"'''
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
-- Indexed but with no declared field kind: bitmap entries could fan out per
-- array element, so coverage cannot be proven exact.
DEFINE INDEX idx_rawcat ON pts FIELDS rawcat;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a', rawcat: 'a' },
	{ id: pts:2, point: [ 20f ], category: 'b', rawcat: 'b' },
	{ id: pts:3, point: [ 30f ], category: 'c', rawcat: 'c' },
	{ id: pts:4, point: [ 40f ], category: 'a', rawcat: 'a' }
];
LET $pt = [0f];
-- OR of coverable equalities: one BitmapOr allow-list branch.
EXPLAIN SELECT id FROM pts
	WHERE (category = 'a' OR category = 'b') AND point <|2,40|> $pt;
-- A negated conjunct stays residual even though its inner predicate is
-- coverable.
EXPLAIN SELECT id FROM pts
	WHERE category = 'a' AND !(category = 'b') AND point <|2,40|> $pt;
-- WITH INDEX pins the plan: no prefilter, the full residual is pushed down.
EXPLAIN SELECT id FROM pts WITH INDEX hn_pt1
	WHERE category = 'a' AND point <|2,40|> $pt;
-- WITH INDEX naming a DIFFERENT index is overridden (with a warning): any
-- driver other than the KnnScan would leave the KNN operator in a
-- never-truthy residual and return zero rows. The hint still disables the
-- prefilter, so the equality is pushed down as the residual.
EXPLAIN SELECT id FROM pts WITH INDEX idx_category
	WHERE category = 'a' AND point <|2,40|> $pt;
-- Undeclared field kind: `rawcat` is indexed but not provably exact.
EXPLAIN SELECT id FROM pts
	WHERE rawcat = 'a' AND point <|2,40|> $pt;
