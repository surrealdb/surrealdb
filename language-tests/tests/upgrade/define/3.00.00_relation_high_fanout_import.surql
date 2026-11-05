/**
[test]
reason = "Setup for relation_high_fanout.surql: the old binary writes one hub vertex whose outbound adjacency holds 1050 edges — wider than the buffer a traversal fills before it has to suspend — so the read on the new binary cannot complete the vertex in a single pass."
run = false
*/

DEFINE TABLE up_fanout_hub SCHEMALESS;
DEFINE TABLE up_fanout_leaf SCHEMALESS;
DEFINE TABLE up_fanout_knows TYPE RELATION FROM up_fanout_hub TO up_fanout_leaf SCHEMALESS;

-- The single source vertex the whole fan-out hangs off.
CREATE up_fanout_hub:h;

-- 1050 targets with contiguous integer ids, so the tail of the fan-out has a
-- nameable id and the traversal's result can be compared against the table.
CREATE |up_fanout_leaf:1..=1050| RETURN NONE;

-- One edge from the hub to every leaf, so the hub's adjacency is 1050 entries
-- wide and every one of them is written by the old binary.
FOR $leaf IN (SELECT VALUE id FROM up_fanout_leaf) {
	RELATE up_fanout_hub:h -> up_fanout_knows -> $leaf;
};
