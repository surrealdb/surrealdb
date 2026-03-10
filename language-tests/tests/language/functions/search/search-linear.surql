/**
[test]

[[test.results]]
value = "[{ embedding: [0.1f, 0.2f, 0.3f], id: test:1, text: 'Graph databases are great.' }]"

[[test.results]]
value = "[{ embedding: [0.05f, 0.1f, 0f], id: test:2, text: 'Relational databases store tables.' }]"

[[test.results]]
value = "[{ embedding: [0.2f, 0.1f, 0.25f], id: test:3, text: 'This document mentions graphs and networks.' }]"

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
value = "[{ distance: 0.0034970067397347426f, ft_score: 0.5366538763046265f, id: test:1, linear_score: 2f }, { distance: 0.05639380312223785f, id: test:3, linear_score: 0f }]"

[[test.results]]
value = "[{ distance: 0.0034970067397347426f, ft_score: 0.5366538763046265f, id: test:1, linear_score: 1.9999999999999953f }, { distance: 0.05639380312223785f, id: test:3, linear_score: -2.000000000000004f }]"

[[test.results]]
value = "[{ id: l:1, linear_score: 1f, score: 10 }, { id: l:3, linear_score: 0.7777777777777778f, score: 8 }, { id: l:2, linear_score: 0.1111111111111111f, score: 2 }]"

*/
-- Sample data ---------------------------------------------------------------
CREATE test:1 SET text = "Graph databases are great.", embedding = [0.10, 0.20, 0.30];
CREATE test:2 SET text = "Relational databases store tables.", embedding = [0.05, 0.10, 0.00];
CREATE test:3 SET text = "This document mentions graphs and networks.", embedding = [0.20, 0.10, 0.25];

-- Analyzer used by the full‑text index
DEFINE ANALYZER simple TOKENIZERS class, punct FILTERS lowercase, ascii;

-- Full‑text index
DEFINE INDEX idx_text ON TABLE test FIELDS text FULLTEXT ANALYZER simple BM25;

-- Vector index (HNSW) on a 3‑dimensional embedding, using cosine distance
DEFINE INDEX idx_embedding ON TABLE test FIELDS embedding HNSW DIMENSION 3 DIST COSINE HASHED_VECTOR;

-- Query vector (whatever your embedding model produced for "graph databases")
LET $qvec = [0.12, 0.18, 0.27];

-- Vector search: top 2 nearest neighbours
LET $vs = SELECT id, vector::distance::knn() as distance FROM test WHERE embedding <|2,100|> $qvec;

-- Full‑text search: top 2 lexical matches
LET $ft = SELECT id, search::score(1) as ft_score FROM test
          WHERE text @1@ 'graph' ORDER BY ft_score DESC LIMIT 2;

-- Fuse with Linear / minmax
RETURN search::linear([$vs, $ft], [2, 1], 2, 'minmax');

-- Fuse with Linear / zscore
RETURN search::linear([$vs, $ft], [2, 1], 2, 'zscore');

-- Linear top-k selection regression
RETURN search::linear(
	[
		[{ id: l:1, score: 10 }, { id: l:2, score: 2 }, { id: l:3, score: 8 }, { id: l:4, score: 1 }]
	],
	[1],
	3,
	'minmax'
);
