/**
[test]
requires = ["diskann"]
issue = 7602
reason = "Public issue surrealdb/surrealdb#7602: a one-shot DiskANN I8 build grew memory until the process was OOM-killed under a systemd memory limit. The growth was the RocksDB block cache, sized from the host's memory because a limit set on the process's own cgroup below the hierarchy root was not read; the DiskANN build's own memory stays flat as the vector count grows. Memory limits cannot be set from a language test: the cgroup reading is pinned by the unit tests in surrealdb/common/src/sys.rs. This pins the build shape of the report, a DISKANN I8 COSINE index defined over existing rows, which builds and answers KNN."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [1, 0, 0, 0], id: chunk:1 }]"

[[test.results]]
value = "[{ embedding: [0, 1, 0, 0], id: chunk:2 }]"

[[test.results]]
value = "[{ embedding: [0, 0, 1, 0], id: chunk:3 }]"

[[test.results]]
value = "[{ embedding: [1, 1, 0, 0], id: chunk:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[{ d: 0f, id: chunk:1 }, { d: 0.2928932309150696f, id: chunk:4 }]"

*/
DEFINE TABLE chunk SCHEMALESS;
CREATE chunk:1 SET embedding = [1, 0, 0, 0];
CREATE chunk:2 SET embedding = [0, 1, 0, 0];
CREATE chunk:3 SET embedding = [0, 0, 1, 0];
CREATE chunk:4 SET embedding = [1, 1, 0, 0];
DEFINE INDEX chunk_diskann ON chunk FIELDS embedding DISKANN DIMENSION 4 DIST COSINE TYPE I8 DEGREE 8 L_BUILD 10 ALPHA 1.2f;
(INFO FOR INDEX chunk_diskann ON chunk).building.status;
SELECT id, vector::distance::knn() AS d FROM chunk WHERE embedding <|2,10|> [1, 0, 0, 0] ORDER BY d;
