/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "KNN prefilter (#548) at 0.1% selectivity: cat1000 = 7 covers 50 of 50k rows, so the allow-list takes the graph-free exact tier — 50 bounded record fetches, guaranteed top-K. Compare knn_hnsw_prefilter_residual_1pct.surql (same dataset, in-traversal path) and knn_hnsw.surql (unfiltered noise floor). Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND cat1000 = 7 ORDER BY dist;
