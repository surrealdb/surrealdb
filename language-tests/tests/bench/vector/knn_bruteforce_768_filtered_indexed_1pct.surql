/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered brute-force KNN, crud-bench's exact reference: cosine similarity aliased and sorted, over `number_idx <= 50` (500 of 50k rows, 1%). The b-tree index yields the 500 matching rows, and each is fetched, decoded and scored. This is what the graph path at 1% has to beat; knn_hnsw_768_filtered_unindexed_1pct is the same predicate through the graph. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM record WHERE number_idx <= 50 ORDER BY _d DESC LIMIT 10;
