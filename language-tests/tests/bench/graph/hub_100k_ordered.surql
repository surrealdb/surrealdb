/**
[bench]
reason = "An ORDER BY id LIMIT 10 lookup over the 100000-degree hub vertex: the planner caps the scan at the limit because a single-table single-direction lookup already emits in ascending edge-id order, so the traversal reads 10 pointer keys instead of 100000. Compare medians against hub_100k_ordered_unpushed, the same sort answered by scanning the full scope."
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 10).id FROM person:1;
