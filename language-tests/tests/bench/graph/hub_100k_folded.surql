/**
[bench]
reason = "The hub_100k_plain traversal after the 100000-edge scope is folded into packed adjacency blocks: the same statement over the same graph, but the merged cursor streams block chunks instead of 100000 individual pointer keys. Compare medians against hub_100k_plain."
run = true
rebuild = false
graph-fold = true
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1;
