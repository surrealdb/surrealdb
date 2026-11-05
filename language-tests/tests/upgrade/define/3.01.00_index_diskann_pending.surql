/**
[env]
imports = ["upgrade/define/3.01.00_index_diskann_pending_import.surql"]

[test]
requires = ["diskann"]
upgrade = true
reason = """
Records an older binary wrote into a DiskANN index after defining it wait in a
pending range, under pending-state guards, until a compaction folds them into
the graph, and the old binary may stop before one has. The newer binary must
find them whether or not it did: an index an older release defined keeps
neither the pending-state summary nor a format whose lookup trusts one, so the
lookup reads its guards and scans every armed shard. A lookup that trusted an
absent summary on such an index would skip the pending records and return
fewer rows, with no error.

The plan assertion and a write on the newer binary live in
3.01.00_index_diskann_pending_current.surql.

Releases in the 3.3.0-beta line before beta.4 refuse an index stamped with the
pre-shared-doc-ID format, and say so through `[[test.repair]]`."""
introduced = ">=3.1.0-beta.3"

[[test.repair]]
since = ">=3.3.0-beta.1, <3.3.0-beta.4"
script = "upgrade/define/3.01.00_index_diskann_pending_rebuild.surql"
unrepaired = "broken"
reason = """
3.3.0-beta.1 through beta.3 moved vector indexes onto a table-level shared
doc-ID space and refuse to serve an index still stamped with the per-index
format, so `REBUILD INDEX` is a required manual step when upgrading onto one
of them. 3.3.0-beta.4 preserves the per-index layout until an explicit
rebuild instead, so no repair is required upgrading onto it or later."""

[[test.results]]
value = "[{ id: pend:1 }]"

[[test.results]]
value = "[{ id: pend:1 }, { id: pend:2 }, { id: pend:3 }]"

*/
SELECT id FROM pend WHERE point <|1,100|> [2.0, 3.0, 4.0, 5.0];
SELECT id FROM pend WHERE point <|3,100|> [2.0, 3.0, 4.0, 5.0] ORDER BY id;
