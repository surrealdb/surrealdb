/**
[env]
imports = ["upgrade/define/3.01.00_index_diskann_compound_id_import.surql"]

[test]
upgrade = true
issue = 7437
introduced = ">=3.1.0-beta.3"
reason = """
The DiskANN half of 3.01.00_index_hnsw_compound_id.surql.

Both ANN families queue writes under a record-keyed pending entry and both
recover the record id from it, so both decoded an id holding a number back
decimalised and lost the row. The two carry the fix separately — one of them
regressed at exactly this seam while every HNSW test stayed green — so the hop
is read back through each.

`dpt:[1, 'a']` is written here and is reachable either side of the repair;
`dpt:[5, 'e']` is written by the importing binary and only after it, because
the doc-ID was recorded under the decimalised spelling before this hop began.

`introduced` matches the sibling DiskANN upgrade test: DISKANN syntax arrived
in 3.1.0-beta.3, and the record-keyed pending layout that carries the defect
is present from that line. `version` bounds the target to the checkout
carrying the fix; a backport must lower it, and the repair's `since`, to its
own line or neither runs there.
"""
version = ">=3.4.0-a"

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.01.00_index_diskann_compound_id_rebuild.surql"
unrepaired = "degraded"
reason = """
A DiskANN index carrying a record whose id holds a number was written with that
id's doc-ID recorded under a decimalised spelling, which addresses no record.
Every KNN hit on it is dropped after taking a top-k slot, so the query returns
fewer rows than the k it asked for rather than failing. Only `REBUILD INDEX`
re-derives the mapping from the records themselves.
"""

[[test.results]]
value = "[{ id: dpt:[1, 'a'], point: [1f, 0f, 0f, 0f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 1f, id: dpt:[1, 'a'] }, { dist: 3f, id: dpt:1 }, { dist: 4f, id: dpt:2 }, { dist: 5f, id: dpt:[5, 'e'] }]"
*/
-- `UPSERT`, not `CREATE`: the repair bracket runs this body twice.
UPSERT dpt:[1, 'a'] SET point = [1.0f, 0.0f, 0.0f, 0.0f];
LET $pt = [0.0f, 0.0f, 0.0f, 0.0f];
-- k exceeds the row count, so a row missing here is one the index dropped.
SELECT id, vector::distance::knn() AS dist FROM dpt WHERE point <|5,100|> $pt ORDER BY dist;
