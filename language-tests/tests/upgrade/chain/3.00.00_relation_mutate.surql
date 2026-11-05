/**
[test]
reason = "Chain mutation for relation.surql: every walked version adds one vertex and RELATEs it to the hub the first version created, so each release appends to an adjacency list an older release built. Both the vertex id and the edge id are generated, so no hop can collide with another."
run = false
*/
LET $friend = (CREATE ONLY chain_rel_person SET hop = $hop, wrote_at = $version).id;
RELATE chain_rel_person:alice -> chain_rel_knows -> $friend SET hop = $hop, wrote_at = $version;
