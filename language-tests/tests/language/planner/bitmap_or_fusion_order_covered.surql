/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "OR bitmap fusion declines when the union satisfies the ORDER BY by composite k-way merge"
issue = 550

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'ml' AND published = true OR category = 'db' AND published = false]\n        UnionIndexScan [ctx: Db] [table: docs, branches: 2, merge_by_index_key_dedup: rank Asc]\n            IndexScan [ctx: Db] [index: idx_cat_pub_rank, access: ['ml', true], direction: Forward]\n            IndexScan [ctx: Db] [index: idx_cat_pub_rank, access: ['db', false], direction: Forward]\n"'''

[[test.results]]
value = "[{ id: docs:2, rank: 1 }, { id: docs:3, rank: 2 }, { id: docs:1, rank: 3 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    SortByKey [ctx: Db] [sort_keys: score ASC]\n        Filter [ctx: Db] [predicate: category = 'ml' AND published = true OR category = 'db' AND published = false]\n            BitmapResolve [ctx: Db] [table: docs]\n                BitmapOr [ctx: Db]\n                    BitmapIndexScan [ctx: Db] [index: idx_cat_pub_rank, access: ['ml', true]]\n                    BitmapIndexScan [ctx: Db] [index: idx_cat_pub_rank, access: ['db', false]]\n"'''
*/

DEFINE FIELD category ON docs TYPE string;
DEFINE FIELD published ON docs TYPE bool;
DEFINE FIELD rank ON docs TYPE number;
DEFINE INDEX idx_cat_pub_rank ON docs FIELDS category, published, rank;

CREATE docs:1 SET category = 'ml', published = true,  rank = 3, score = 0.2 RETURN NONE;
CREATE docs:2 SET category = 'ml', published = true,  rank = 1, score = 0.9 RETURN NONE;
CREATE docs:3 SET category = 'db', published = false, rank = 2, score = 0.5 RETURN NONE;
CREATE docs:4 SET category = 'db', published = true,  rank = 4, score = 0.1 RETURN NONE;

-- Every branch pins the same composite-index prefix and the ordering is
-- that index's next column: the union satisfies ORDER BY rank with a k-way
-- merge and no Sort, so bitmap fusion declines and the streaming plan keeps
-- its sort elimination.
EXPLAIN SELECT id FROM docs WHERE (category = 'ml' AND published = true) OR (category = 'db' AND published = false) ORDER BY rank;
SELECT id, rank FROM docs WHERE (category = 'ml' AND published = true) OR (category = 'db' AND published = false) ORDER BY rank;

-- An ordering the composite cannot merge on installs a Sort either way:
-- the OR fuses.
EXPLAIN SELECT id FROM docs WHERE (category = 'ml' AND published = true) OR (category = 'db' AND published = false) ORDER BY score;
