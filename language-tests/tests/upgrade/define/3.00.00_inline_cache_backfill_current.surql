/**
[env]
imports = ["upgrade/define/3.00.00_relation_import.surql", "upgrade/define/3.00.00_field_reference_import.surql"]

[test]
upgrade = true
reason = """
An inline sidecar cache is materialised by the first write that is eligible for
it, and it is built by scanning the adjacency or reference keys already on
disk. When those keys were written by an older release, that scan is a reader
of the older release's layout — and the value it materialises then answers
every later fan-out read on that record in place of the scan. A key the
backfill drops, duplicates or decodes wrongly therefore stops being a transient
scan artefact and becomes a permanently wrong answer, returned without an
error.

Every other inline-cache test builds its fixture with the same binary that
reads it, so the cache and the keys it summarises are always in one format.
Only a fixture written by a released binary drives the backfill over the
layouts those releases actually produce: a vertex-side pointer key that embeds
no target, which has no cache representation at all and must make the cache
abstain rather than serve a truncated neighbour set; and reference keys whose
trailing identity bytes have to equal, byte for byte, the identity the delete
path recomputes when a referencing record goes away.

The deletions are the load-bearing half. A cached entry is retracted by
recomputing its identity and matching it against the stored entry, so a
one-byte disagreement between an old key's bytes and the recomputed ones makes
the retraction miss silently: every read taken before the delete still looks
perfect, and the deleted record stays visible for the life of the cache. The
reads that follow each delete are the only place that is observable.

The `ALTER` statements are part of what is under test rather than mere setup:
they rewrite a table definition an older release wrote, adding a cap field that
release's catalog revision had no slot for, and the caps must take effect over
the records that definition already governs.

The seed axis is what this file is for. It sends a 3.0.x, 3.1.x or 3.2.x writer
straight to the current checkout, so the backfill runs over the keys that
release itself laid down with no intermediate binary in between to relaunder
them. A cumulative walk cannot express this at all: every hop rewrites the
adjacency it maintains, and the caps only exist on the last binary in the walk.

The body is gated to the current checkout as the upgrade target because the cap
clauses are spelled only by this build's parser. That costs no coverage the
seed axis provides.

Nothing here asserts cache state, which is unobservable from SurrealQL and must
stay so: the cache is an accelerator whose answers are required to equal the
scan's, and every assertion below is written against the scan's answer."""
version = ">=3.4.0-a"
introduced = ">=3.0.0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:bob, person:dave]"

[[test.results]]
value = "2"

[[test.results]]
value = "[book:1, book:2, book:4]"

[[test.results]]
value = "[person:alice]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[book:2, book:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:dave]"

*/

-- Turn the caps on over tables the importing binary defined and populated. The
-- caches themselves stay empty until a write makes a record eligible.
ALTER TABLE person INLINE EDGES 32;
ALTER TABLE author INLINE REFERENCES 32;

-- One eligible write per side, each on a record that already carries edges or
-- references the importing binary wrote, so each backfill has to read that
-- binary's keys to build a complete set.
CREATE person:dave RETURN NONE;
RELATE person:alice -> knows:k3 -> person:dave RETURN NONE;
CREATE book:4 SET title = 'W', author = author:a RETURN NONE;

-- Every edge on the hub, old and new, resolved exactly once: an old key the
-- backfill dropped, duplicated, or decoded as carrying a target it does not
-- have all show up here.
SELECT VALUE array::sort(->knows->person) FROM ONLY person:alice;
-- The same fan-out counted rather than resolved, which separates a duplicated
-- edge from a reordered one; the resolved form alone conflates them.
SELECT VALUE array::len(->knows) FROM ONLY person:alice;

-- The reference side of the same property: the back-link set is screened
-- against the raw trailing bytes of the reference keys the importing binary
-- wrote, so a misaligned prefix silently yields only the new book, or nothing.
SELECT VALUE array::sort(<~book) FROM ONLY author:a;

-- A vertex no eligible write has touched has no cache of its own and must
-- still traverse, so a fan-out spanning cached and uncached records answers
-- uniformly.
SELECT VALUE array::sort(<-knows<-person) FROM ONLY person:bob;

-- Retracting a reference the importing binary wrote: the identity recomputed
-- at delete time has to match the bytes the backfill lifted from that key, or
-- the removal misses and the deleted book stays in every later answer.
DELETE book:1;
SELECT VALUE array::sort(<~book) FROM ONLY author:a;

-- The same removal identity on the adjacency side, against an edge the
-- importing binary wrote.
DELETE knows:k1;
SELECT VALUE array::sort(->knows->person) FROM ONLY person:alice;
