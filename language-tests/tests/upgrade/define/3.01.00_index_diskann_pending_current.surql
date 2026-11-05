/**
[env]
imports = ["upgrade/define/3.01.00_index_diskann_pending_import.surql"]

[test]
upgrade = true
reason = """
The plan behind 3.01.00_index_diskann_pending.surql's reads, and a write on the
newer binary into the index an older one defined: the newer binary arms the
guard of the record's shard, and maintains a pending-state summary the index's
format does not have it trust, so the lookup still finds the record through the
guards. Gated to the current checkout as the upgrade target, since neither
`EXPLAIN` output nor the write fits a body carrying a `[[test.repair]]`."""
version = ">=3.4.0-a"
introduced = ">=3.1.0-beta.3"

[[test.results]]
match = '''string::contains($result, "KnnScan") AND string::contains($result, "dann_pend")'''

[[test.results]]
value = "[{ id: pend:4, point: [2f, 3f, 4f, 6f] }]"

[[test.results]]
value = "[{ id: pend:4 }]"

[[test.results]]
value = "[{ id: pend:1 }, { id: pend:2 }, { id: pend:3 }, { id: pend:4 }]"

*/
EXPLAIN SELECT id FROM pend WHERE point <|1,100|> [2.0, 3.0, 4.0, 5.0];
CREATE pend:4 SET point = [2.0, 3.0, 4.0, 6.0];
SELECT id FROM pend WHERE point <|1,100|> [2.0, 3.0, 4.0, 5.0];
SELECT id FROM pend WHERE point <|4,100|> [2.0, 3.0, 4.0, 5.0] ORDER BY id;
