/**
[env]
imports = ["bench/util/embeddings-diskann-prefilter-5000.surql"]

[bench]
reason = "DiskANN KNN prefilter (#548) at 1% selectivity: cat100 = 7 covers 50 of 5000 rows, so the allow-list takes the graph-free exact tier with zero in-traversal record fetches. Compare against the same query on the undeclared `rawcat` column (edit the probe column) or knn_diskann_filtered_selective.surql for the in-traversal baseline."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND cat100 = 7 ORDER BY dist;
