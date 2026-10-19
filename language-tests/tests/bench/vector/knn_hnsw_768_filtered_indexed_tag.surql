/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered HNSW KNN, top-10 at ef 64, with the string-tag equality `status_idx = 'published'` (16667 of 50k rows, 33%) on a column declared TYPE string with a b-tree index — crud-bench's tag leg with filter_index. At 33% the allow-list is far over the exact-tier threshold, so this is the graph tier with a string-keyed bitmap. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE embedding <|10,64|> $q AND status_idx = 'published';
