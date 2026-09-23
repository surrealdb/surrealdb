/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/3.01.00_index_hnsw_compound_id_import.surql"]

[test]
upgrade = true
issue = 7437
introduced = "3.1.0"
reason = """
A record id holding a number inside an array or object must be reachable
through an ANN index, and an index that already holds one written under the
old spelling must say so until it is rebuilt.

The pending queue recovers the record id from the entry it is coalesced under.
The key spells it with the codec used for indexed values, which omits a
number's kind, so an id read back from there alone comes back decimalised and
addresses no record: the KNN hit is dropped at the record fetch, after it has
taken a top-k slot. The id is now carried in the pending value, which keeps it
exactly.

That fixes what a new write records, not what an old one left behind. A record
the importing binary indexed under such an id had its doc-ID recorded under the
decimalised spelling before this hop began, and nothing short of `REBUILD INDEX`
repairs it — which is what `[[test.repair]]` declares, and what the release
notes have to say. So the body reads both: `cpt:[1, 'a']`, written here, is
reachable either side of the repair, and `cpt:[5, 'e']`, written by the older
binary, only after it.

A rebuild alone is not what makes the repair work. Before the shared
table-level doc-ID space, the mapping was index-scoped and a rebuild re-derived
it; from 3.3 it is table-scoped and shared with every other index on the table,
so a rebuild leaves the damaged mapping in place. What rejects it is the
forward lookup verifying a hit against the reverse mapping, which is why this
test sits with the injective doc-ID key rather than with the pending-value fix:
on that change alone a `3.2.3` source repairs and a `3.3.0-beta.3` source does
not. A backport to a line whose doc-ID space is still index-scoped may take the
test without the key.

`introduced` is the release that first wrote the record-keyed pending layout:
3.0.x queued vectors under a layout carrying the id in its value, so a store
that release wrote has nothing to repair and would not show the damage this
declares. `version` bounds the target to the checkout carrying the fix, which
also keeps this clear of 3.3.0-beta.1 through beta.3, where a pre-shared-doc-ID
index is refused outright rather than degraded; beta.4 onwards preserves it. A backport must lower both bounds to its
own line or the test silently never runs there.
"""
version = ">=3.3.0-beta.5"

[[test.repair]]
since = ">=3.3.0-beta.5"
script = "upgrade/define/3.01.00_index_hnsw_compound_id_rebuild.surql"
unrepaired = "degraded"
reason = """
An ANN index carrying a record whose id holds a number was written with that
id's doc-ID recorded under a decimalised spelling, which addresses no record.
Every KNN hit on it is dropped after taking a top-k slot, so the query returns
fewer rows than the k it asked for rather than failing. Only `REBUILD INDEX`
re-derives the mapping from the records themselves, so it is a required manual
step for any index holding such an id.
"""

[[test.results]]
value = "[{ id: cpt:[1, 'a'], point: [1, 0, 0, 0] }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KnnScan") AND string::contains($result, "hnsw_cpt")'''

[[test.results]]
value = "[{ dist: 1f, id: cpt:[1, 'a'] }, { dist: 3f, id: cpt:1 }, { dist: 4f, id: cpt:2 }, { dist: 5f, id: cpt:[5, 'e'] }]"

[[test.results]]
value = "[true]"
*/
-- `UPSERT`, not `CREATE`: the repair bracket runs this body twice.
UPSERT cpt:[1, 'a'] SET point = [1, 0, 0, 0];
LET $pt = [0, 0, 0, 0];
-- The rows below are only evidence about the index if the index is what served
-- them; a scan fallback would answer the same query.
EXPLAIN SELECT id FROM cpt WHERE point <|5,100|> $pt;
-- k exceeds the row count, so a row missing here is one the index dropped.
SELECT id, vector::distance::knn() AS dist FROM cpt WHERE point <|5,100|> $pt ORDER BY dist;
-- Expected ids are type-blind: `[1, 'a']` is satisfied by `[1dec, 'a']`, so a
-- defect that decimalised an id without dropping its row would pass above. These
-- read the id back through the index rather than off the record, so the variant
-- asserted is the one the index resolved. Only the id this body writes is
-- read: the one the older binary wrote is absent until the repair runs, and
-- this body runs on both sides of it.
SELECT VALUE type::is_int(id[0]) FROM cpt WHERE point <|1,100|> [1, 0, 0, 0];
