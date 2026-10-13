/**
[env]
imports = ["bench/util/embeddings-hnsw-prefilter-50000.surql"]

[bench]
reason = "KNN prefilter (#548) at 90% selectivity: flag90 = true covers 45000 of 50k rows — a barely-selective allow-list still gating graph admission (below the 100k threshold where ef boosting stops; admission stays bitmap-gated at every size). Guards against the prefilter regressing near-universe filters, where building the bitmap is most work for least benefit. Export SURREAL_HNSW_BUILD_SEED for graph-identical A/B runs."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND flag90 = true ORDER BY dist;
