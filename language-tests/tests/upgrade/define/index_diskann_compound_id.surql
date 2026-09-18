/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/index_diskann_compound_id_import.surql"]

[test]
upgrade = true
issue = 7437
reason = """
A record id holding a number inside an array or object must be reachable
through a DiskANN index the upgraded binary writes into.

This is the writing direction: the older binary builds the index holding only
plain ids, and the upgraded one writes the compound ones. The reading
direction — a compound id the *older* binary wrote, which needs the mapping
rebuilt before it resolves — has no pair on this line; see below.

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
under the decimalised spelling before this hop began. That mapping belongs to
the index, so `REBUILD INDEX` clears it and derives it again from the records —
which is what the upgrade note tells an operator to do. This runner has no
repair bracket to declare that in, so the reading direction is not covered here
and this pair asserts only on ids the upgraded binary writes.

DiskANN arrived in 3.1.0-beta.3, which is what `importing-version` names: this
runner strips a source's prerelease unless the requirement carries one, so a
plain `>=3.1.0` would admit 3.1.0-beta.2 and the import's `DEFINE INDEX …
DISKANN` would fail on a binary whose parser has no such keyword.

Like `index_diskann.surql` beside it, that leaves this pair inert: the newest
source in `LANG_UPGRADE_VERSIONS` is 3.1.0-beta.2, so no hop satisfies the gate.
It starts running when a release at or past 3.1.0-beta.3 is added to that list.

`version` bounds the target to the checkout carrying the fix, so the pairwise
hops between older releases skip it. 3.2.5 is this line's unreleased version, so
it selects the checkout and no released binary satisfies it; re-check that
against the tags if this is ported further. This line hops adjacent pairs only —
there is no seed axis — so the pair runs on whatever hop reaches the checkout.
"""
# DiskANN arrived in 3.1.0-beta.3, so an older source has no such index to build
# and the hop cannot mean anything. `importing-version` is this line's gate for
# that, and it must name the prerelease: the runner strips a source's prerelease
# unless the requirement carries one.
importing-version = ">=3.1.0-beta.3"
version = ">=3.2.5"

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
-- `UPSERT`, not `CREATE`: re-runnable bodies cost nothing and keep the option
-- open for a runner that replays them.
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
