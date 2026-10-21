/**
[env]
imports = ["upgrade/define/3.01.00_index_diskann_import.surql"]

[test]
upgrade = true
reason = """
An index that silently becomes unusable falls back to a table scan and returns
exactly the same rows, so the read assertions in index_diskann.surql cannot tell a
working index from a missing one. This asserts the plan, and then writes a
record so the new binary has to maintain the structure the old one built.

Both live here rather than in index_diskann.surql because neither can span the whole
3.x line. `EXPLAIN` output is not stable across it — older releases return an
array of plan objects with entirely different operator names — and the write
cannot go in a body carrying a `[[test.repair]]`, since that body is run twice
against one store and the first pass's write would be visible to the second
pass's reads. Gating to the current checkout as the upgrade target costs no
coverage that matters: the seed axis runs every released version straight to it.

The delete that follows removes the record the old binary seeded the graph
with, its entry point. The old binary wrote the graph state at revision 1,
with no start points beside the entry point; the new binary must retire that
entry point from the state it reads, and write it back at revision 2, without
losing the graph.

pts:5 then shares pts:2's vector, so the element the old binary wrote for that
vector holds two records: the new binary resolves both from it, and once pts:2
is deleted, only pts:5."""
version = ">=3.4.0-a"
introduced = ">=3.1.0-beta.3"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KnnScan") AND string::contains($result, "dann_pts")'''

[[test.results]]
value = "[{ id: pts:4, point: [2f, 3f, 4f, 6f] }]"

[[test.results]]
value = "[{ id: pts:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:4 }]"

[[test.results]]
value = "[{ id: pts:5, point: [10f, 20f, 30f, 40f] }]"

[[test.results]]
value = "[{ id: pts:2 }, { id: pts:5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:5 }]"

*/
LET $pt = [2.0, 3.0, 4.0, 5.0];
EXPLAIN SELECT id FROM pts WHERE point <|1,100|> $pt;
-- pts:4 sits one unit from the probe, closer than pts:1, so it must take over
-- as nearest once the new binary has indexed it.
CREATE pts:4 SET point = [2.0, 3.0, 4.0, 6.0];
SELECT id FROM pts WHERE point <|1,100|> $pt;
-- pts:1 seeded the old binary's graph, so it is the graph's entry point.
DELETE pts:1 RETURN NONE;
SELECT id FROM pts WHERE point <|1,100|> [1.0, 2.0, 3.0, 4.0];
CREATE pts:5 SET point = [10.0, 20.0, 30.0, 40.0];
SELECT id FROM pts WHERE point <|2,100|> [10.0, 20.0, 30.0, 40.0] ORDER BY id;
DELETE pts:2 RETURN NONE;
SELECT id FROM pts WHERE point <|1,100|> [10.0, 20.0, 30.0, 40.0];
