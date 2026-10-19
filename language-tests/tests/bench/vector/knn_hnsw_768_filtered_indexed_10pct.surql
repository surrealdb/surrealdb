/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Filtered HNSW KNN, top-10 at ef 64, with `number_idx <= 500` (5000 of 50k rows, 10%) on the typed, indexed column. The allow-list is over the exact-tier threshold, so the pre-filter keeps the graph walk and gates admission on bitmap membership with a boosted ef. Dataset, and what it mirrors of crud-bench: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE embedding <|10,64|> $q AND number_idx <= 500;
