/**
[env]
imports = ["bench/util/embeddings-hnsw-cosine-2000.surql"]

[bench]
reason = "Top-10 nearest neighbours via the cosine HNSW index (768-dim)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,40|> array::map(array::range(0, 768), |$i| rand::float(0.0, 1.0)) ORDER BY dist;
