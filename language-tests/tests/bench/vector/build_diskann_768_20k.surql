/**
[env]
imports = ["bench/util/embeddings-data-768d-20000.surql"]

[bench]
reason = "Build a cosine DiskANN index over 20000 768-dim vectors at the DDL defaults (DEGREE 64, L_BUILD 100, ALPHA 1.2). Twenty times the rows of build_diskann_768, so the drain runs some twenty bounded compaction frames rather than one and the storage layer is doing real work between them. Run it as `cargo make bench -- vector/build_diskann_768_20k --backend rocksdb` and compare branches within one backend. build_hnsw_cosine, which a DiskANN change leaves untouched, is the noise floor on `mem` only: it does not set `rebuild-file-backend`, so a file-backend run skips it. The graph still fits the 256 MiB DiskANN cache at this size — the cache-miss regime is what diag_diskann_build_rocksdb_200k measures, and a build that size cannot be re-run per bench iteration. Few samples on purpose: every iteration re-imports 20000 768-dim rows before building, so each one costs about a minute even on `mem`."
run = true
rebuild = true
rebuild-file-backend = true
warmup = "1s"
sample-size = 3
measurement-time = "60s"
max-time = "420s"

[test]
run = false
*/

DEFINE INDEX emb_diskann ON embeddings FIELDS vec DISKANN DIMENSION 768 DIST COSINE TYPE F32;
