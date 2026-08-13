/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "KNN prefilter (#548) at 1% selectivity — the headline acceptance bench: cat100 = 7 covers 500 of 50k rows on the exact tier with zero in-traversal record fetches. The >=10x acceptance comparison is this bench vs knn_hnsw_prefilter_residual_1pct.surql: identical dataset, identical graph, same 1% selectivity, differing only in whether the filter column's declared kind lets the equality become an allow-list. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs. For the issue's 1M-row figure, scale array::range(0, 50000) in bench/util/embeddings-hnsw-prefilter-50000.surql (manual, not CI)."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND cat100 = 7 ORDER BY dist;
