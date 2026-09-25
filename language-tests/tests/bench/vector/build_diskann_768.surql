/**
[env]
imports = ["bench/util/embeddings-data-768d-1000.surql"]

[bench]
reason = "Build a cosine DiskANN index over 1000 768-dim vectors at the DDL defaults (DEGREE 64, L_BUILD 100, ALPHA 1.2) — the small end of the build-time curve, sized like build_hnsw_cosine so the two index kinds can be read side by side. One bounded compaction frame drains the whole corpus at this size, so what it measures is the per-vector cost of the insert itself; build_diskann_768_20k is the one that exercises the drain across many frames."
run = true
rebuild = true
rebuild-file-backend = true
warmup = "1s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

DEFINE INDEX emb_diskann ON embeddings FIELDS vec DISKANN DIMENSION 768 DIST COSINE TYPE F32;
