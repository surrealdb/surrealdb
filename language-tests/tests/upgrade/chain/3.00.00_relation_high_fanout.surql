/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The first version
writes a hub whose outbound adjacency is already wider than a traversal can
carry in one pass; every later version appends one more edge to that same
adjacency and then traverses the whole of it.

A pair shows one release reading a wide fan-out that a single other release
wrote, so every entry in the adjacency has the same shape. This shows the
fan-out staying exactly-once while release after release appends to it: the
adjacency ends up holding whatever each version in turn produced, interleaved
with the seed entries, and a traversal still has to suspend part-way through
the vertex, resolve what it buffered, and resume without re-emitting or
skipping anything at the boundary. Getting that wrong under a mixture of
entry shapes is not reachable from a pair, where a boundary always falls
between entries one writer produced.

Every hop asserts the full target set, the edge count read from the edge table,
the tail of the seed fan-out — which lies past the suspend point — reachable
inbound, and one hop stamp per version walked, so a release that quietly
stopped appending is caught in the same pass."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_relation_high_fanout_setup.surql"
mutate = "upgrade/chain/3.00.00_relation_high_fanout_mutate.surql"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result == [1010 + $hop, true]'''

[[test.results]]
match = '''$result[0].count == 1010 + $hop'''

[[test.results]]
value = "[chain_fanout_hub:h]"

[[test.results]]
match = '''
    array::len($result) == 1010 + $hop
    && array::sort(array::distinct($result)) == array::range(0, $hop + 1)
'''

*/

-- Every target the hub's outbound adjacency yields, in one traversal that has
-- to span the seed fan-out plus everything appended since.
LET $seen = (SELECT VALUE ->chain_fanout_knows->chain_fanout_leaf FROM ONLY chain_fanout_hub:h);

-- The same set taken from the target table rather than the graph, so the
-- comparison below has an independent reference to check against.
LET $leaves = (SELECT VALUE id FROM chain_fanout_leaf);

-- The length grows by exactly one per hop, and the set equality pins that every
-- target — seed or appended — was recovered as itself, none twice.
RETURN [array::len($seen), (array::sort($seen) == array::sort($leaves))];

-- Reading the edge table directly agrees with the traversal, so a wrong count
-- above is a traversal fault and not a missing edge record.
SELECT count() FROM chain_fanout_knows GROUP ALL;

-- The last edge the first version wrote lies past the point where a traversal
-- has to suspend, and stays reachable from the other end after every hop.
SELECT VALUE <-chain_fanout_knows<-chain_fanout_hub FROM ONLY chain_fanout_leaf:1010;

-- Hop 0 stamps the whole seed fan-out; every version after it stamps exactly
-- one more edge, so each release in turn really did write through this
-- adjacency rather than only the last one.
SELECT VALUE hop FROM chain_fanout_knows;
