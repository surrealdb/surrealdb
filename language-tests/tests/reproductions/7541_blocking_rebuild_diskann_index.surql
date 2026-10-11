/**
[env]

[test]
reason = "A blocking REBUILD INDEX of a DiskANN I8 index returns once the index is ready, and the rebuilt index answers KNN queries."
issue = 7541

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ building: { compacting: false, initial: 128, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ d: 0f, id: t:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ building: { compacting: false, initial: 128, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ d: 0f, id: t:5 }]"

*/
INSERT INTO t array::map(array::range(0, 128), |$i| { id: $i, v: [$i, 0, 0, 0] }) RETURN NONE;
DEFINE INDEX ix ON t FIELDS v DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE I8;

-- A standalone blocking rebuild: commits its definition, then waits for the build.
REBUILD INDEX ix ON t;
INFO FOR INDEX ix ON t;
SELECT id, vector::distance::knn() AS d FROM t WHERE v <|1,40|> [5, 0, 0, 0] ORDER BY d;

-- Inside a transaction the rebuild still waits for its build before COMMIT.
BEGIN;
REBUILD INDEX ix ON t;
COMMIT;
INFO FOR INDEX ix ON t;
SELECT id, vector::distance::knn() AS d FROM t WHERE v <|1,40|> [5, 0, 0, 0] ORDER BY d;
