/**
[env]
imports = ["bench/util/embeddings-data-768d-1000.surql"]

[bench]
reason = "Build a cosine HNSW index over 1000 768-dim vectors — measures index construction, where caching stored-vector norms removes most of the per-comparison magnitude work"
run = true
rebuild = true
warmup = "1s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 768 DIST COSINE TYPE F32 EFC 200 M 12;
