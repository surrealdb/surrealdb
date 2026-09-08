/**
[env]
imports = ["upgrade/define/3.00.00_index_hnsw_import.surql"]

[test]
upgrade = true
reason = """
An HNSW index written by an older binary keeps its own doc-ID space until it is
explicitly rebuilt, so KNN must traverse the graph the old binary wrote and
return the same neighbours in the same order.

The post-upgrade write lives in 3.00.00_index_hnsw_current.surql: a body carrying a
`[[test.repair]]` is run twice, so it cannot mutate the state it reads.

Releases in the 3.3.0-beta line are the exception and say so through
`[[test.repair]]`: they reject an index stamped with the pre-shared-doc-ID
format instead of reading it."""
# Skipping 3.0.0 as the from-version: the post-upgrade KNN query returns the
# full table with NONE distances (i.e., index empty / not used). Plausible
# cause is async HNSW build on 3.0.0 leaving the on-disk graph unwritten when
# the process exits, but the root cause hasn't been isolated — reconfirm
# before relaxing this gate. KNN coverage still runs on every later hop.
importing-version = ">=3.0.1"

[[test.repair]]
since = ">=3.3.0-beta.1, <3.4.0"
script = "upgrade/define/3.00.00_index_hnsw_rebuild.surql"
unrepaired = "broken"
reason = """
The 3.3.0 betas moved vector indexes onto a table-level shared doc-ID space and
refuse to serve an index still stamped with the per-index format, so
`REBUILD INDEX` is a required manual step when upgrading onto one of them."""

[[test.results]]
match = '''string::starts_with($result.indexes.hnsw_pts, "DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

*/
INFO FOR TABLE pts;
LET $pt = [2, 3, 4, 5];
-- Traverse the graph the OLD binary built, with no repair in between.
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt ORDER BY dist;
