/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered HNSW KNN, top-10 at ef 64, with `number_idx <= 50` (500 of 50k rows, 1%) on a column declared TYPE int with a b-tree index — crud-bench's filter_index variant. The range becomes an allow-list of 500 rows, under the exact-tier threshold, so the pre-filter scores the matching rows exactly instead of walking the graph. Same graph and rows as knn_hnsw_768_filtered_unindexed_1pct, which is the A/B. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE embedding <|10,64|> $q AND number_idx <= 50;
