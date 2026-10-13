/**
[test]
reason = "Chain setup for relation_high_fanout.surql: the first walked version writes a hub whose outbound adjacency is already wider than the buffer a traversal fills before it has to suspend, so every later version appends to an adjacency that spans a suspend-and-resume boundary. Its edges carry hop 0, which the body counts, and its leaves take contiguous integer ids so the tail of the seed fan-out has a nameable id."
run = false
*/

DEFINE TABLE chain_fanout_hub SCHEMALESS;
DEFINE TABLE chain_fanout_leaf SCHEMALESS;
DEFINE TABLE chain_fanout_knows TYPE RELATION FROM chain_fanout_hub TO chain_fanout_leaf SCHEMALESS;

-- The single source vertex every later version appends to.
CREATE chain_fanout_hub:h SET wrote_at = $version;

-- 1010 seed targets, enough that the seed fan-out alone already crosses the
-- boundary and stays across it for the whole walk.
CREATE |chain_fanout_leaf:1..=1010| SET hop = 0, wrote_at = $version RETURN NONE;

-- One seed edge per leaf, all of them written by the first version.
FOR $leaf IN (SELECT VALUE id FROM chain_fanout_leaf) {
	RELATE chain_fanout_hub:h -> chain_fanout_knows -> $leaf SET hop = 0, wrote_at = $version;
};
