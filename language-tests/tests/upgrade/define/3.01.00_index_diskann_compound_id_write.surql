/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/3.01.00_index_diskann_compound_id_write_import.surql"]

[test]
introduced = ">=3.1.0-beta.3"
upgrade = true
issue = 7437
reason = """
A record id holding a number inside an array or object must be reachable
through a DiskANN index the upgraded binary writes into.

This is the writing direction: the older binary builds the index holding only
plain ids, and the upgraded one writes the compound ones. The reading
direction — a compound id the *older* binary wrote, which needs the mapping
rebuilt before it resolves — is a separate pair.

The pending queue recovers the record id from the entry it is coalesced under.
The key spells the id with the codec used for indexed values, which omits a
number's kind, so an id read back from there alone comes back decimalised and
addresses no record: the KNN hit is dropped at the record fetch, after it has
taken a top-k slot. The id is therefore carried in the pending value, which
keeps it exactly, and this is the hop that proves a binary carrying it can
write such a record into an index an older one built.

The index is built by the older binary and never rebuilt here, so the write
lands in a queue and a graph that release created. Records the *older* binary
wrote under such an id are a separate matter: their doc-ID mapping was recorded
under the decimalised spelling before this hop began, and the mapping is
table-scoped, so it outlives the index. Repairing it needs a forward lookup that
rejects the stale entry so a rebuild re-derives it, which is a separate change
with its own pairs. That is why this one asserts on ids the upgraded binary
writes rather than declaring a repair.

DiskANN arrived in 3.1.0, so the seed axis starts there rather than at 3.0.x.

`version` bounds the target to the checkout carrying the fix, so the pairwise
hops between older releases skip it; the seed axis sends every released version
straight to the current checkout, which is the hop that matters. It names the
prerelease that carries the fix rather than the line's `.0`, because an
unanchored `>=3.3.0` also schedules an adjacent pair whose target is an earlier
3.3.0 beta, which does not carry it and fails. A backport must lower that bound
to its own line, to a release that carries the fix, or the test silently never
runs there.
"""
version = ">=3.3.0-beta.5"

[[test.results]]
value = "[{ id: dcp:[3, 'c'], point: [1, 0, 0, 0] }]"

[[test.results]]
value = "[{ id: dcp:{ k: 4 }, point: [2, 0, 0, 0] }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KnnScan") AND string::contains($result, "diskann_cpt")'''

[[test.results]]
value = "[{ dist: 1f, id: dcp:[3, 'c'] }, { dist: 2f, id: dcp:{ k: 4 } }, { dist: 3f, id: dcp:1 }, { dist: 4f, id: dcp:2 }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true]"
*/
-- `UPSERT`, not `CREATE`: the repair bracket runs a body twice, and writing
-- these as re-runnable keeps that option open for whoever declares one.
UPSERT dcp:[3, 'c'] SET point = [1, 0, 0, 0];
UPSERT dcp:{k: 4}   SET point = [2, 0, 0, 0];
LET $pt = [0, 0, 0, 0];
-- The rows below are only evidence about the index if the index is what served
-- them; a scan fallback would answer the same query.
EXPLAIN SELECT id FROM dcp WHERE point <|4,100|> $pt;
-- k exceeds the row count, so a row missing here is one the index dropped.
SELECT id, vector::distance::knn() AS dist FROM dcp WHERE point <|4,100|> $pt ORDER BY dist;
-- Expected ids are type-blind: `dcp:[3, 'c']` is satisfied by `dcp:[3dec, 'c']`, so a
-- defect that decimalised an id without dropping its row would pass above. These
-- read the id back through the index rather than off the record, so the variant
-- asserted is the one the index resolved -- reading it from storage would pass
-- whatever the index returned.
SELECT VALUE type::is_int(id[0]) FROM dcp WHERE point <|1,100|> [1, 0, 0, 0];
SELECT VALUE type::is_int(id.k) FROM dcp WHERE point <|1,100|> [2, 0, 0, 0];
