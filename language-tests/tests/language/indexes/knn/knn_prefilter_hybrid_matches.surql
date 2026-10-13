/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Pre-filtered vector search (#548), hybrid search — the epic's motivating query (#550): vector KNN AND full-text MATCHES AND two b-tree equalities fuse into one allow-list bitmap (FTS posting list ∩ index bitmaps) gating the ANN search, with BM25 scoring applied to the survivors and no predicate evaluated in-traversal."
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, score, dist]\n    Compute [ctx: Db] [fields: score = search::score(...)]\n        SortByKey [ctx: Db] [sort_keys: dist ASC]\n            Compute [ctx: Db] [fields: dist = vector::distance::knn(...)]\n                Filter [ctx: Db] [predicate: body @1@ 'attention' AND category = 'ml' AND published = true]\n                    KnnScan [ctx: Db] [index: hn_emb, k: 2, ef: 40, dimension: 1]\n                        BitmapAnd [ctx: Db]\n                            BitmapFullTextScan [ctx: Db] [index: ft_body, query: attention]\n                            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml']\n                            BitmapIndexScan [ctx: Db] [index: idx_published, access: = true]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, score, dist] {rows: 2}\n    Compute [ctx: Db] [fields: score = search::score(...)] {rows: 2}\n        SortByKey [ctx: Db] [sort_keys: dist ASC] {rows: 2}\n            Compute [ctx: Db] [fields: dist = vector::distance::knn(...)] {rows: 2}\n                Filter [ctx: Db] [predicate: body @1@ 'attention' AND category = 'ml' AND published = true] {rows: 2}\n                    KnnScan [ctx: Db] [index: hn_emb, k: 2, ef: 40, dimension: 1, prefilter_tier: exact] {rows: 2}\n                        BitmapAnd [ctx: Db] {rows: 2}\n                            BitmapFullTextScan [ctx: Db] [index: ft_body, query: attention] {rows: 4}\n                            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'ml'] {rows: 7}\n                            BitmapIndexScan [ctx: Db] [index: idx_published, access: = true] {rows: 8}\n\nTotal rows: 2"'''

[[test.results]]
value = "[{ dist: 10f, id: docs:1, score: 0.2889266014099121f }, { dist: 50f, id: docs:5, score: 0.35173675417900085f }]"
*/

DEFINE FIELD category ON docs TYPE string;
DEFINE FIELD published ON docs TYPE bool;
DEFINE INDEX idx_category ON docs FIELDS category;
DEFINE INDEX idx_published ON docs FIELDS published;
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_body ON docs FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX hn_emb ON docs FIELDS embedding HNSW DIMENSION 1;

CREATE docs:1 SET category = 'ml', published = true,  body = 'transformer attention is all you need', embedding = [ 10f ] RETURN NONE;
CREATE docs:2 SET category = 'ml', published = false, body = 'attention mechanisms in vision',        embedding = [ 20f ] RETURN NONE;
CREATE docs:3 SET category = 'db', published = true,  body = 'roaring bitmaps and attention',         embedding = [ 30f ] RETURN NONE;
CREATE docs:4 SET category = 'ml', published = true,  body = 'convolution networks',                  embedding = [ 40f ] RETURN NONE;
CREATE docs:5 SET category = 'ml', published = true,  body = 'attention for hybrid retrieval',        embedding = [ 50f ] RETURN NONE;
-- Filler corpus so the matched term is rare enough for a positive BM25
-- score (idf is clamped at zero when a term appears in most documents).
CREATE docs:6 SET category = 'ml', published = true,  body = 'gradient descent methods',   embedding = [ 60f ] RETURN NONE;
CREATE docs:7 SET category = 'db', published = true,  body = 'log structured merge trees', embedding = [ 70f ] RETURN NONE;
CREATE docs:8 SET category = 'ml', published = false, body = 'dropout and regularisation', embedding = [ 80f ] RETURN NONE;
CREATE docs:9 SET category = 'db', published = true,  body = 'write ahead logging',        embedding = [ 90f ] RETURN NONE;
CREATE docs:10 SET category = 'ml', published = true, body = 'batch normalisation layers', embedding = [ 100f ] RETURN NONE;

LET $q = [ 0f ];
-- The fused plan: KnnScan admission driven by
-- posting(attention) ∩ bitmap(category) ∩ bitmap(published); nothing
-- evaluated in-traversal.
EXPLAIN SELECT id, search::score(1) AS score, vector::distance::knn() AS dist FROM docs
	WHERE embedding <|2,40|> $q
		AND body @1@ 'attention'
		AND category = 'ml'
		AND published = true
	ORDER BY dist;
EXPLAIN ANALYZE SELECT id, search::score(1) AS score, vector::distance::knn() AS dist FROM docs
	WHERE embedding <|2,40|> $q
		AND body @1@ 'attention'
		AND category = 'ml'
		AND published = true
	ORDER BY dist;
-- docs:1 and docs:5 are the matching survivors nearest to the query
-- (docs:2 unpublished, docs:3 wrong category, docs:4 no match).
SELECT id, search::score(1) AS score, vector::distance::knn() AS dist FROM docs
	WHERE embedding <|2,40|> $q
		AND body @1@ 'attention'
		AND category = 'ml'
		AND published = true
	ORDER BY dist;
