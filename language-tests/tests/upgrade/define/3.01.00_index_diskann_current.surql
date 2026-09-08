/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
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
coverage that matters: the seed axis runs every released version straight to it."""
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

*/
LET $pt = [2.0, 3.0, 4.0, 5.0];
EXPLAIN SELECT id FROM pts WHERE point <|1,100|> $pt;
-- pts:4 sits one unit from the probe, closer than pts:1, so it must take over
-- as nearest once the new binary has indexed it.
CREATE pts:4 SET point = [2.0, 3.0, 4.0, 6.0];
SELECT id FROM pts WHERE point <|1,100|> $pt;
