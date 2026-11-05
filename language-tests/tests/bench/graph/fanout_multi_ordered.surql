/**
[bench]
reason = "An ORDER BY id LIMIT 2 lookup as the final hop of a fused traversal: the collapsed `->knows->person` hop hands the capped scan a batch of ~10 mid-hop sources at once, so the per-source ordered cap and the multi-source concurrent fan-out run together — each source's scan stops after 2 pointer keys instead of its full degree while the fan-out overlaps the per-source cursors. Compare medians against fanout_multi_ordered_unpushed, the same question with the cap declined."
run = true
rebuild = false
datasets = { dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person->(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM person:1..=500;
