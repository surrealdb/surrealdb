/**
[env]
imports = ["upgrade/define/index_hnsw_import.surql"]

[test]
upgrade = true
reason = "An HNSW index built by an older version uses the retired per-index doc-ID format and must be rebuilt after upgrade onto the shared table-level doc-ID space; REBUILD INDEX restores it and KNN returns neighbors in the correct order"
# Skipping 3.0.0 as the from-version: the post-upgrade KNN query returns the
# full table with NONE distances (i.e., index empty / not used). Plausible
# cause is async HNSW build on 3.0.0 leaving the on-disk graph unwritten when
# the process exits, but the root cause hasn't been isolated — reconfirm
# before relaxing this gate. KNN coverage still runs on every later hop.
importing-version = ">=3.0.1"

[[test.results]]
match = '''string::starts_with($result.indexes.hnsw_pts, "DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

*/
INFO FOR TABLE pts;
LET $pt = [2, 3, 4, 5];
-- An index built before the shared table-level doc-ID space must be rebuilt
-- after upgrade before it can be queried.
REBUILD INDEX hnsw_pts ON pts;
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt;
