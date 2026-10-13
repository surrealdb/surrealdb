/**
[env]
imports = ["bench/util/embeddings-diskann-5000.surql"]

[bench]
reason = "Top-10 nearest neighbours via the DiskANN index. The query vector is bound with LET: the ANN index is only selected when the right-hand side of `<|k,ef|>` resolves to a value at planning time, so an inline call expression would plan a full table scan with the KNN conjunct evaluated per row instead."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

LET $q = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0));
SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,40|> $q ORDER BY dist;
