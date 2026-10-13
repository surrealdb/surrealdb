/**
[env]
imports = ["upgrade/define/3.01.00_index_diskann_import.surql"]

[test]
upgrade = true
reason = """
A DiskANN index written by an older binary keeps its own doc-ID space until it
is explicitly rebuilt, so KNN must traverse the graph the old binary wrote.

The post-upgrade write lives in 3.01.00_index_diskann_current.surql: a body carrying a
`[[test.repair]]` is run twice, so it cannot mutate the state it reads.

3.3.0-beta.1 through beta.3 are the exception and say so through
`[[test.repair]]`: they reject an index stamped with the pre-shared-doc-ID
format instead of reading it."""
# DISKANN syntax was introduced in 3.1.0-beta.3. `importing-version` is the
# feature-introduction gate: a run is expanded only when the *writing* binary
# supports the syntax, and since a hop's target is never older than its source,
# the reading side is covered automatically.
introduced = ">=3.1.0-beta.3"

[[test.repair]]
since = ">=3.3.0-beta.1, <3.3.0-beta.4"
script = "upgrade/define/3.01.00_index_diskann_rebuild.surql"
unrepaired = "broken"
reason = """
3.3.0-beta.1 through beta.3 moved vector indexes onto a table-level shared
doc-ID space and refuse to serve an index still stamped with the per-index
format, so `REBUILD INDEX` is a required manual step when upgrading onto one
of them. 3.3.0-beta.4 preserves the per-index layout until an explicit
rebuild instead, so no repair is required upgrading onto it or later."""

[[test.results]]
match = '''string::starts_with($result.indexes.dann_pts, "DEFINE INDEX dann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }]"

*/
INFO FOR TABLE pts;
LET $pt = [2.0, 3.0, 4.0, 5.0];
-- Nearest neighbour out of the graph the OLD binary built, with no repair in
-- between: pts:1 is closest to [2,3,4,5].
SELECT id FROM pts WHERE point <|1,100|> $pt;
