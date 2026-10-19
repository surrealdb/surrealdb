/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered DiskANN KNN, top-10 at l_search 200, with `number <= 50` (500 of 50k rows, 1%) on a schemaless, unindexed column — the DiskANN twin of knn_hnsw_768_filtered_unindexed_1pct, evaluating the predicate per visited candidate. Compare knn_diskann_768_filtered_indexed_1pct. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record_dk WHERE embedding <|10,200|> $q AND number <= 50;
