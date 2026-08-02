/**
[env]
imports = ["bench/util/embeddings-hnsw-filtered-5000.surql"]

[bench]
reason = "Top-10 nearest neighbours via the HNSW index with a selective residual filter (category = 7, 2% of rows). The filter is pushed into the HNSW search, so the traversal fetches each candidate's record to evaluate the condition, exercising the per-neighbourhood record-fetch batching. The filtered search keeps only *matching* elements in its result window, so its stopping radius is the distance to the EF-th match; with 20 matching graph elements (far below any EF here) the window never fills and the traversal is exhaustive — EF and K are not the dials on this bench, filter selectivity is. The dataset and the query vector are deterministic, so running both sides of a before/after with SURREAL_HNSW_BUILD_SEED set builds an identical graph and isolates the code change. Compare against knn_hnsw.surql (unfiltered) as the noise floor."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND category = 7 ORDER BY dist;
