/**
[env]
imports = ["upgrade/define/3.00.00_relation_high_fanout_import.surql"]

[test]
upgrade = true
reason = """
A single vertex's outbound adjacency, written 1050 edges wide by the old
binary, must traverse to exactly its own 1050 targets on the new one — every
target present, none of them twice.

When an adjacency entry names only the edge and not the far vertex, the
traversal recovers each target through the edge's own adjacency. Those
recoveries are buffered and the buffer is bounded, so a fan-out wider than the
bound forces the scan to suspend part-way through the vertex, drain what it
holds, and resume immediately after the last entry it consumed. Where it
resumes is observable only in the result: resuming inclusively re-emits the
boundary target and the count comes back one too high, resuming past an
unconsumed entry drops the tail and it comes back short. 1050 edges put exactly
one such boundary inside the vertex, with a short remainder after it.

The narrow relation pair cannot reach this. Three edges never fill the buffer,
so the suspend-and-resume path is never entered at all and a boundary error
stays invisible; only the volume brings it out. The seed axis is where it bites
hardest, since it hands the current checkout an adjacency an old release wrote
with no binary in between to rewrite the layout."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1050, true]"

[[test.results]]
match = '''$result[0].count == 1050'''

[[test.results]]
value = "[up_fanout_hub:h]"

*/

-- Every target the hub's outbound adjacency yields, in one traversal that has
-- to span the whole fan-out.
LET $seen = (SELECT VALUE ->up_fanout_knows->up_fanout_leaf FROM ONLY up_fanout_hub:h);

-- The same set taken from the target table rather than the graph, so the
-- comparison below has an independent reference to check against.
LET $leaves = (SELECT VALUE id FROM up_fanout_leaf);

-- The length pins exactly-once emission across the fan-out, and the set
-- equality pins that each of the 1050 targets was recovered as itself, with
-- none substituted for another.
RETURN [array::len($seen), (array::sort($seen) == array::sort($leaves))];

-- Reading the edge table directly agrees with the traversal, so a wrong count
-- above is a traversal fault and not a missing edge record.
SELECT count() FROM up_fanout_knows GROUP ALL;

-- The last edge of the fan-out sits past the point where the traversal has to
-- suspend, and is still reachable from the other end.
SELECT VALUE <-up_fanout_knows<-up_fanout_hub FROM ONLY up_fanout_leaf:1050;
