/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "The inexact-prefilter baseline at 1% selectivity on the prefilter dataset: rawcat's index holds an array, so the allow-list rawcat = 7 builds (500 of 50k rows) is a superset and the conjunct is judged on each member (allow-list plus residual). knn_hnsw_prefilter_sel01.surql is the same query on cat100, whose index holds one entry per record, so its allow-list is exact. Same dataset, same graph, same selectivity. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND rawcat = 7 ORDER BY dist;
