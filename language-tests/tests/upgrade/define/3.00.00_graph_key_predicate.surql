/**
[env]
imports = ["upgrade/define/3.00.00_graph_key_predicate_import.surql"]

[test]
upgrade = true
reason = """
A filtered graph traversal can be answered two ways. Where the predicate reads
only the endpoints or the edge id, those values are already present in the
adjacency key, so the reader can decide the filter from the key bytes alone and
never open the edge record. Where the predicate reads any other field, it has
to resolve the record.

Which of the two runs is decided by the layout the *writing* release produced.
An adjacency entry that embeds its target admits the key-only answer; one
written without a target cannot, and the same query silently falls back to the
record. Both arms therefore have to agree, over bytes no current build wrote,
or a filtered traversal starts returning a different edge set depending only on
which release happened to lay the graph down — with no error to mark it.

The filters below are written in the parenthesised `->(edge WHERE ...)` form
deliberately. A trailing `->edge[WHERE ...]` parses as a separate idiom part
applied after the traversal, which reaches none of this: the predicate never
reaches the lookup, so the test would pass on any layout and prove nothing.

The seed axis is what exercises this properly, sending a 3.0.x, 3.1.x or 3.2.x
writer straight to the current checkout so each released layout drives the
reader with no intermediate binary to relaunder it."""
introduced = ">=3.0.0"

[[test.results]]
value = "[up_kpred_knows:o3]"

[[test.results]]
value = "[up_kpred_knows:i2]"

[[test.results]]
value = "[up_kpred_knows:o1]"

[[test.results]]
value = "[up_kpred_knows:o2, up_kpred_knows:o3]"

[[test.results]]
value = "[up_kpred_knows:o2]"

[[test.results]]
value = "2"

[[test.results]]
value = "[up_kpred_person:t2, up_kpred_person:t3]"

*/
-- Endpoint predicate outbound: decided from the writer's key bytes where the
-- entry embeds its target, and from the edge record where it does not.
SELECT VALUE ->(up_kpred_knows WHERE out = up_kpred_person:t3) FROM ONLY up_kpred_person:hub;
-- The same filter inbound. This is the only assertion that catches an in/out
-- orientation swap in the key decode.
SELECT VALUE <-(up_kpred_knows WHERE in = up_kpred_person:t4) FROM ONLY up_kpred_person:hub;
-- A predicate on the edge id alone, the one shape a target-less entry can still
-- answer without opening the record.
SELECT VALUE ->(up_kpred_knows WHERE id = up_kpred_knows:o1) FROM ONLY up_kpred_person:hub;
-- Negated, so a filter that inverts its sense fails here rather than passing by
-- symmetry with the positive cases above.
SELECT VALUE array::sort(->(up_kpred_knows WHERE out != up_kpred_person:t1)) FROM ONLY up_kpred_person:hub;
-- Both endpoints at once, so a reader that satisfies one and drops the other is
-- caught.
SELECT VALUE ->(up_kpred_knows WHERE in = up_kpred_person:hub AND out = up_kpred_person:t2) FROM ONLY up_kpred_person:hub;
-- A bidirectional traversal always resolves records; pinning it against the
-- directed assertions above holds the two paths to the same answer.
SELECT VALUE array::len(<->(up_kpred_knows WHERE out = up_kpred_person:hub)) FROM ONLY up_kpred_person:hub;
-- A non-key field forces the record path outright, and the traversal continues
-- through to the far vertex.
SELECT VALUE array::sort(->(up_kpred_knows WHERE score > 5)->up_kpred_person) FROM ONLY up_kpred_person:hub;
