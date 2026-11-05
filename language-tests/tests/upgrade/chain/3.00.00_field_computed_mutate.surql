/**
[test]
reason = "Chain mutation for field_computed.surql: every walked version creates one row through the field definitions the first version wrote. The record id is generated and the computed value derives from the hop, so no two hops collide and each row records which binary computed it."
run = false
*/
CREATE chain_field_computed SET name = { first: 'Hop', last: <string> $hop }, hop = $hop, wrote_at = $version;
-- The per-hop rewrite of the seed row the first walked version created. It lives
-- here rather than in the body because the body is graded at every hop and must
-- not mutate the state its own assertions read.
UPDATE chain_field_computed:seed SET revision += 1;
-- Rewriting a dependency of the computed field must make this binary recompute
-- it, through a VALUE clause an earlier binary wrote.
UPDATE chain_field_computed:seed SET name.last = string::concat('Rev', <string> revision);
