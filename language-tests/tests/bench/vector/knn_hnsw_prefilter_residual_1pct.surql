/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "The pre-#548 in-traversal baseline at 1% selectivity on the prefilter dataset: rawcat has no declared field kind, so rawcat = 7 (500 of 50k rows) is not bitmap-coverable and is evaluated per visited candidate inside the HNSW traversal — one record fetch per candidate. The >=10x acceptance comparison is knn_hnsw_prefilter_sel01.surql vs this bench: same dataset, same graph, same selectivity, no cross-branch or cross-engine confounds. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND rawcat = 7 ORDER BY dist;
