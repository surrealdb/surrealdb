/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The RELATION table, the
hub vertex and one seed edge are written by the first version; every later
version appends another edge to the same adjacency list and then traverses the
whole graph.

An adjacent pair can only show that the release after the writer reads the edge.
This shows the graph key family staying coherent while release after release
appends to it: the seed edge the very first binary wrote is still traversable in
both directions after every subsequent binary has written through the same
adjacency, and the neighbour list holds exactly one edge per version walked."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_relation_setup.surql"
mutate = "upgrade/chain/3.00.00_relation_mutate.surql"

[[test.results]]
match = '''
    string::contains($result.tables.chain_rel_knows, "DEFINE TABLE chain_rel_knows TYPE RELATION")
    && string::contains($result.tables.chain_rel_knows, "IN chain_rel_person")
    && string::contains($result.tables.chain_rel_knows, "OUT chain_rel_person")
'''

[[test.results]]
match = '''$result[0].count == $hop + 1'''

[[test.results]]
match = '''array::len($result) == $hop + 1 && array::len(array::distinct($result)) == $hop + 1'''

[[test.results]]
value = "[chain_rel_person:bob]"

[[test.results]]
value = "[chain_rel_person:alice]"

[[test.results]]
match = '''array::sort($result) == array::range(0, $hop + 1)'''

*/
-- The RELATION typing the first version wrote is still on the table.
INFO FOR DB;
-- One seed edge plus one edge per version walked so far.
SELECT count() FROM chain_rel_knows GROUP ALL;
-- Every edge any version appended is still reachable from the hub, and each
-- lands on a distinct vertex.
SELECT VALUE ->chain_rel_knows->chain_rel_person FROM ONLY chain_rel_person:alice;
-- The edge the first version wrote is still traversable outbound...
SELECT VALUE ->chain_rel_knows[WHERE hop = 0]->chain_rel_person FROM ONLY chain_rel_person:alice;
-- ...and inbound, so both halves of the adjacency survived every hop.
SELECT VALUE <-chain_rel_knows<-chain_rel_person FROM ONLY chain_rel_person:bob;
-- Hops 0..$hop each stamped exactly once: proof that every release in turn
-- appended to the graph, not just the last one.
SELECT VALUE hop FROM chain_rel_knows;
