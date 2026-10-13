/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "KNN prefilter (#548) at 10% selectivity: cat10 = 7 covers 5000 of 50k rows (> the 2000 exact threshold) so the allow-list gates graph traversal with boosted ef — zero in-traversal record fetches. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND cat10 = 7 ORDER BY dist;
