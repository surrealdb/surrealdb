/**
[env]

[test]
reason = "Bitmap candidate fusion (roaring bitmap index intersection) plan selection, EXPLAIN shape and results"
issue = 547

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
value = "[{ id: docs:1 }, { id: docs:4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
        BitmapScan [ctx: Db] [table: docs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''

[[test.results]]
value = "[{ id: docs:1, rank: 3 }, { id: docs:4, rank: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, rank]
    Limit [ctx: Db] [limit: 1]
        Sort [ctx: Db] [sort_keys: rank ASC, keep: 1]
            Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
                BitmapScan [ctx: Db] [table: docs]
                    BitmapAnd [ctx: Db]
                        BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                        BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''

[[test.results]]
value = "[{ id: docs:1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true AND body @@ 'attention']
        BitmapScan [ctx: Db] [table: docs]
            BitmapAnd [ctx: Db]
                BitmapFullTextScan [ctx: Db] [index: ft_body, query: attention]
                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''

[[test.results]]
value = "[{ id: docs:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND not(published = true)]
        BitmapScan [ctx: Db] [table: docs]
            BitmapAndNot [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''

[[test.results]]
value = "[{ id: docs:4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true AND !(body @@ 'attention')]
        BitmapScan [ctx: Db] [table: docs]
            BitmapAndNot [ctx: Db]
                BitmapAnd [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                    BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
                BitmapFullTextScan [ctx: Db] [index: ft_body, query: attention]
"'''

[[test.results]]
value = "[{ id: docs:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: docs, direction: Forward, predicate: !(published = true), pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: docs:1 }, { id: docs:3 }, { id: docs:4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: published = true AND (category = 'ml' OR category = 'db')]
        BitmapScan [ctx: Db] [table: docs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
                BitmapOr [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                    BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'db']
"'''

[[test.results]]
value = "[{ id: docs:3, score: 0.8472978472709656f }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, score]
    Filter [ctx: Db] [predicate: published = true AND body @1@ 'roaring']
        BitmapScan [ctx: Db] [table: docs]
            BitmapAnd [ctx: Db]
                BitmapFullTextScan [ctx: Db] [index: ft_body, query: roaring]
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
        IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'ml', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
            IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'ml', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'ml', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
        IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'ml', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 2}
    Filter [ctx: Db] [predicate: category = 'ml' AND published = true] {rows: 2}
        BitmapScan [ctx: Db] [table: docs] {rows: 2}
            BitmapAnd [ctx: Db] {rows: 2}
                BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml'] {rows: 3}
                BitmapIndexScan [ctx: Db] [index: idx_published, access: = true] {rows: 3}

Total rows: 2"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 5]
        Aggregate [ctx: Db] [by: category]
            Filter [ctx: Db] [predicate: category = 'ml' AND published = true]
                BitmapScan [ctx: Db] [table: docs]
                    BitmapAnd [ctx: Db]
                        BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']
                        BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]
"'''
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

-- Two AND-ed indexable equality predicates, no ORDER BY: bitmap plan.
SELECT id FROM docs WHERE category = 'ml' AND published = true;
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND published = true;

-- ORDER BY on an unindexed field needs a Sort anyway: bitmap plan feeds it.
SELECT id, rank FROM docs WHERE category = 'ml' AND published = true ORDER BY rank;
EXPLAIN SELECT id, rank FROM docs WHERE category = 'ml' AND published = true ORDER BY rank LIMIT 1;

-- A full-text MATCHES branch composes with the b-tree branches; BM25 scoring
-- is not needed to build the candidate set.
SELECT id FROM docs WHERE category = 'ml' AND published = true AND body @@ 'attention';
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND published = true AND body @@ 'attention';

-- Conjunctive NOT becomes a bitmap subtraction (and_not); the column has a
-- declared scalar kind so the subtracted bitmap is exact.
SELECT id FROM docs WHERE category = 'ml' AND NOT (published = true);
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND NOT (published = true);

-- NOT on a full-text MATCHES branch subtracts the posting bitmap.
SELECT id FROM docs WHERE category = 'ml' AND published = true AND !(body @@ 'attention');
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND published = true AND !(body @@ 'attention');

-- A standalone NOT never produces a bitmap plan (no positive anchor): table scan.
SELECT id FROM docs WHERE !(published = true);
EXPLAIN SELECT id FROM docs WHERE !(published = true);

-- An OR conjunct nested in the AND becomes a bitmap union member.
SELECT id FROM docs WHERE published = true AND (category = 'ml' OR category = 'db');
EXPLAIN SELECT id FROM docs WHERE published = true AND (category = 'ml' OR category = 'db');

-- search::score() runs over the surviving rows of a bitmap plan.
SELECT id, search::score(1) AS score FROM docs WHERE published = true AND body @1@ 'roaring';
EXPLAIN SELECT id, search::score(1) AS score FROM docs WHERE published = true AND body @1@ 'roaring';

-- ORDER BY id is covered by any equality index scan: streaming keeps sort
-- elimination, no bitmap plan.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND published = true ORDER BY id;

-- LIMIT without ORDER BY: streaming early-termination wins, no bitmap plan.
EXPLAIN SELECT id FROM docs WHERE category = 'ml' AND published = true LIMIT 1;

-- Single indexable predicate: single-index streaming plan, no bitmap plan.
EXPLAIN SELECT id FROM docs WHERE category = 'ml';

-- WITH INDEX pins the plan: no bitmap plan.
EXPLAIN SELECT id FROM docs WITH INDEX idx_category WHERE category = 'ml' AND published = true;

-- EXPLAIN ANALYZE reports per-node candidate cardinalities.
EXPLAIN ANALYZE SELECT id FROM docs WHERE category = 'ml' AND published = true;

-- A LIMIT above a blocking GROUP BY cannot terminate the source early:
-- bitmap fusion still applies.
EXPLAIN SELECT category, count() FROM docs WHERE category = 'ml' AND published = true GROUP BY category LIMIT 5;
