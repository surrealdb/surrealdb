/**
[test]
reason = "Chain mutation for index_diskann.surql: every walked version inserts one vector into the graph an earlier version built. The vector is placed $hop units from the probe, so distance grows with the hop and the nearest neighbour stays the vector the FIRST walked version wrote — which is what makes the graph's oldest entry the thing under test at every later hop."
run = false
*/
CREATE chain_pts SET point = [0.0, 0.0, 0.0, <float> $hop], hop = $hop, wrote_at = $version;
