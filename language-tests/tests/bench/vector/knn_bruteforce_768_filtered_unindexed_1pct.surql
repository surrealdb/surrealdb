/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered brute-force KNN over `number <= 50` on the schemaless, unindexed column — crud-bench's bruteforce leg without filter_index. The plan is a full TableScan of 50k rows, but its pre-decode filter tests the predicate on the encoded record and decodes only the 500 matching rows, so it lands near knn_bruteforce_768_filtered_indexed_1pct rather than at the cost of decoding every 768-d row; a change that stopped the scan from filtering before decoding shows here as a large regression. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM record WHERE number <= 50 ORDER BY _d DESC LIMIT 10;
