/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Top-level OR bitmap union fusion: plan selection, EXPLAIN shape and results"
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
value = "[{ id: docs:1 }, { id: docs:2 }, { id: docs:4 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'ml' OR published = false]\n        BitmapResolve [ctx: Db] [table: docs]\n            BitmapOr [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']\n                BitmapIndexScan [ctx: Db] [index: idx_published, access: = false]\n"'''

[[test.results]]
value = "[{ id: docs:3 }, { id: docs:4 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'db' OR body @@ 'convolution']\n        BitmapResolve [ctx: Db] [table: docs]\n            BitmapOr [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'db']\n                BitmapFullTextScan [ctx: Db] [index: ft_body, query: convolution]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'db' OR published = false OR body @@ 'convolution']\n        BitmapResolve [ctx: Db] [table: docs]\n            BitmapOr [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'db']\n                BitmapIndexScan [ctx: Db] [index: idx_published, access: = false]\n                BitmapFullTextScan [ctx: Db] [index: ft_body, query: convolution]\n"'''

[[test.results]]
value = "[{ id: docs:3, score: 0.8472978472709656f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'ml' OR category = 'db']\n        UnionIndexScan [ctx: Db] [table: docs, branches: 2]\n            IndexScan [ctx: Db] [index: idx_category, access: = 'ml', direction: Forward]\n            IndexScan [ctx: Db] [index: idx_category, access: = 'db', direction: Forward]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: category = 'ml' OR published = false]\n        UnionIndexScan [ctx: Db] [table: docs, branches: 2, merge_by_id: Asc]\n            IndexScan [ctx: Db] [index: idx_category, access: = 'ml', direction: Forward]\n            IndexScan [ctx: Db] [index: idx_published, access: = false, direction: Forward]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Limit [ctx: Db] [limit: 1]\n        Filter [ctx: Db] [predicate: category = 'ml' OR published = false]\n            UnionIndexScan [ctx: Db] [table: docs, branches: 2]\n                IndexScan [ctx: Db] [index: idx_category, access: = 'ml', direction: Forward]\n                IndexScan [ctx: Db] [index: idx_published, access: = false, direction: Forward]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    TableScan [ctx: Db] [table: docs, direction: Forward, predicate: category = 'ml' OR rank = 4, pre_decode_filter: yes]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    TableScan [ctx: Db] [table: docs, direction: Forward, predicate: category = 'ml' OR published = false, pre_decode_filter: yes]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 3}\n    Filter [ctx: Db] [predicate: category = 'ml' OR published = false] {rows: 3}\n        BitmapResolve [ctx: Db] [table: docs] {rows: 3}\n            BitmapOr [ctx: Db] {rows: 3}\n                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml'] {rows: 3}\n                BitmapIndexScan [ctx: Db] [index: idx_published, access: = false] {rows: 1}\n\nTotal rows: 3"'''

[[test.results]]
value = '''"Limit [ctx: Db] [limit: 5]\n    Aggregate [ctx: Db] [by: category]\n        Filter [ctx: Db] [predicate: category = 'ml' OR published = false]\n            BitmapResolve [ctx: Db] [table: docs]\n                BitmapOr [ctx: Db]\n                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']\n                    BitmapIndexScan [ctx: Db] [index: idx_published, access: = false]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Limit [ctx: Db] [limit: 2]\n        SortTopKByKey [ctx: Db] [sort_keys: rank ASC, limit: 2]\n            Filter [ctx: Db] [predicate: category = 'ml' OR published = false]\n                BitmapResolve [ctx: Db] [table: docs]\n                    BitmapOr [ctx: Db]\n                        BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']\n                        BitmapIndexScan [ctx: Db] [index: idx_published, access: = false]\n"'''

*/

DEFINE FIELD category ON docs TYPE string;
DEFINE FIELD published ON docs TYPE bool;
DEFINE INDEX idx_category ON docs FIELDS category;
DEFINE INDEX idx_published ON docs FIELDS published;
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_body ON docs FIELDS body FULLTEXT ANALYZER simple BM25;

CREATE docs:1 SET category = 'ml', published = true,  body = 'transformer attention is all you need', rank = 3 RETURN NONE;
CREATE docs:2 SET category = 'ml', published = false, body = 'attention mechanisms in vision', rank = 1 RETURN NONE;
CREATE docs:3 SET category = 'db', published = true,  body = 'roaring bitmaps and attention', rank = 2 RETURN NONE;
CREATE docs:4 SET category = 'ml', published = true,  body = 'convolution networks', rank = 4 RETURN NONE;

-- Two OR-ed indexable equality predicates, no ORDER BY / LIMIT: the union
-- resolves as one bitmap. docs:2 satisfies both branches and appears once —
-- dedupe is structural, not a HashSet.
SELECT id FROM docs WHERE category = 'ml' OR published = false;
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR published = false;

-- A full-text MATCHES branch unions with a b-tree branch.
SELECT id FROM docs WHERE category = 'db' OR body @@ 'convolution';
EXPLAIN SELECT id FROM docs WHERE category = 'db' OR body @@ 'convolution';

-- OR trees flatten into a single union.
EXPLAIN SELECT id FROM docs WHERE category = 'db' OR published = false OR body @@ 'convolution';

-- search::score() runs over the surviving rows of an OR bitmap plan.
SELECT id, search::score(1) AS score FROM docs WHERE category = 'db' OR body @1@ 'roaring';

-- Both branches empty: the fused union resolves to no rows.
SELECT id FROM docs WHERE category = 'nope' OR body @@ 'xyzzy';

-- Same-index equality branches with distinct values are record-disjoint:
-- there are no duplicate fetches for a fused plan to save, so the streaming
-- union keeps the query.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR category = 'db';

-- ORDER BY keeps the union scan's sorted-merge and sort-elimination plans.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR published = false ORDER BY id;

-- LIMIT without ORDER BY keeps streaming early termination.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR published = false LIMIT 1;

-- An unindexed branch cannot union: the whole OR falls back.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR rank = 4;

-- WITH INDEX pins the plan.
EXPLAIN SELECT id FROM docs WITH INDEX idx_category WHERE category = 'ml' OR published = false;

-- EXPLAIN ANALYZE reports per-branch candidate cardinalities.
EXPLAIN ANALYZE SELECT id FROM docs WHERE category = 'ml' OR published = false;

-- A LIMIT above a blocking GROUP BY cannot terminate the source early: the
-- OR still fuses.
EXPLAIN SELECT category, count() FROM docs WHERE category = 'ml' OR published = false GROUP BY category LIMIT 5;

-- An ORDER BY nothing covers installs a Sort above either plan: the OR
-- fuses even under a LIMIT, and the Sort runs over the bitmap resolve.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' OR published = false ORDER BY rank LIMIT 2;
