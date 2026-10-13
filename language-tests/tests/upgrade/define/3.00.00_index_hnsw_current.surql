/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/3.00.00_index_hnsw_import.surql"]

[test]
upgrade = true
reason = """
An index that silently becomes unusable falls back to a table scan and returns
exactly the same rows, so the read assertions in index_hnsw.surql cannot tell a
working index from a missing one. This asserts the plan, and then writes a
record so the new binary has to maintain the structure the old one built.

Both live here rather than in index_hnsw.surql because neither can span the whole
3.x line. `EXPLAIN` output is not stable across it — older releases return an
array of plan objects with entirely different operator names — and the write
cannot go in a body carrying a `[[test.repair]]`, since that body is run twice
against one store and the first pass's write would be visible to the second
pass's reads. Gating to the current checkout as the upgrade target costs no
coverage that matters: the seed axis runs every released version straight to it."""
version = ">=3.4.0-a"
importing-version = ">=3.0.1"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KnnScan") AND string::contains($result, "hnsw_pts")'''

[[test.results]]
value = "[{ id: pts:5, point: [2, 3, 4, 6] }]"

[[test.results]]
value = "[{ dist: 1f, id: pts:5 }, { dist: 2f, id: pts:1 }]"

*/
LET $pt = [2, 3, 4, 5];
EXPLAIN SELECT id FROM pts WHERE point <|2,100|> $pt;
-- pts:5 sits one unit from the probe, so a correctly maintained graph must
-- return it ahead of pts:1.
CREATE pts:5 SET point = [2, 3, 4, 6];
SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt ORDER BY dist;
