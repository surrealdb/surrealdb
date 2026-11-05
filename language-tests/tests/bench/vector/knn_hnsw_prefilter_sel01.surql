/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "KNN prefilter (#548) at 1% selectivity — the headline acceptance bench: cat100 = 7 covers 500 of 50k rows, so the allow-list takes the graph-free exact tier. cat100's index holds one entry per record, so the allow-list is exact and the conjunct leaves the per-member judge; knn_hnsw_prefilter_residual_1pct.surql is the same query on rawcat, whose index holds an array and so keeps the conjunct in the residual. Same dataset, same graph, same 1% selectivity. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs. For the issue's 1M-row figure, scale array::range(0, 50000) in bench/util/embeddings-hnsw-prefilter-50000.surql (manual, not CI)."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND cat100 = 7 ORDER BY dist;
