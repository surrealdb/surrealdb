/**
[env]

[test]
reason = "Regression (#548): a MATCHES conjunct combined with a KNN operator must not return zero rows. MATCHES conjuncts are never pushed in-traversal: exactly coverable ones join the allow-list bitmap (query 1); non-coverable shapes (a negated MATCHES, query 2) are enforced by the outer Filter through the physical MATCHES operator."
issue = 548

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
value = "NONE"

[[test.results]]
value = "[{ dist: 10f, id: docs:1 }, { dist: 30f, id: docs:3 }]"

[[test.results]]
value = "[{ dist: 20f, id: docs:2 }]"
*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_body ON docs FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX hn_emb ON docs FIELDS embedding HNSW DIMENSION 1;
CREATE docs:1 SET body = 'transformer attention', embedding = [ 10f ] RETURN NONE;
CREATE docs:2 SET body = 'convolution networks',  embedding = [ 20f ] RETURN NONE;
CREATE docs:3 SET body = 'attention is all',      embedding = [ 30f ] RETURN NONE;
LET $q = [ 0f ];
-- Covered MATCHES: returned zero rows before #548.
SELECT id, vector::distance::knn() AS dist FROM docs
	WHERE embedding <|2,40|> $q AND body @@ 'attention'
	ORDER BY dist;
-- Negated MATCHES cannot join the allow-list; it is excluded from the
-- in-traversal residual and enforced by the outer Filter instead.
SELECT id, vector::distance::knn() AS dist FROM docs
	WHERE embedding <|2,40|> $q AND !(body @@ 'attention')
	ORDER BY dist;
