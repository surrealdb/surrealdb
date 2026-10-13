/**
[bench]
reason = "Cache tier under the numeric-ids configuration: the fanout_10k cached dataset answers every batch from the inline caches, so the cursor never opens, no scope is ever observed and nothing folds — this guards that enabling the configuration does not tax the pure-cache path. Compare medians against fanout_10k's cached side."
run = true
rebuild = false
graph-numeric-ids = true
datasets = { cached = "bench/util/graph-fanout-10000-cached.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1..=10000;
