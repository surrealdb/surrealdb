/**
[bench]
reason = "One-hop fan-out over a single 100000-degree vertex on the unfolded (per-key delta) adjacency layout — the baseline side of the hub_100k family, against which hub_100k_folded and hub_100k_numeric measure what packed blocks and numeric entries save at extreme degree."
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1;
