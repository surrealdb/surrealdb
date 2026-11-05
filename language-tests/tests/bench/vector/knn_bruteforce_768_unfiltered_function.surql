/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Unfiltered brute-force KNN spelled as crud-bench's bruteforce leg spells it: the top 10 of 50k 768-d rows by `vector::similarity::cosine(embedding, $q)`, an `ORDER BY` over a projected score with a `LIMIT`. Each row calls the builtin, so this measures the function path a KNN operator does not take. Dataset: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 30
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM record ORDER BY _d DESC LIMIT 10;
