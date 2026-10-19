/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered HNSW KNN, top-10 at ef 64, with `number <= 50` (500 of 50k rows, 1%) on a schemaless, unindexed column — crud-bench's default variant. Nothing can pre-filter the predicate, so it is evaluated per visited graph candidate, each one a full record fetch and decode. Compare knn_hnsw_768_filtered_indexed_1pct (same query, same graph, typed + indexed twin column) and knn_bruteforce_768_filtered_indexed_1pct (exact top-10 over the 500 matching rows). Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 30
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE embedding <|10,64|> $q AND number <= 50;
