/**
[test]
reason = "Chain mutation for relation_high_fanout.surql: every walked version adds one leaf and RELATEs it to the hub the first version created, so each release in turn appends to an adjacency that is already wider than one traversal pass. The leaf id is generated, so no hop can collide with another or with the integer-keyed seed leaves."
run = false
*/

LET $leaf = (CREATE ONLY chain_fanout_leaf SET hop = $hop, wrote_at = $version).id;
RELATE chain_fanout_hub:h -> chain_fanout_knows -> $leaf SET hop = $hop, wrote_at = $version;
