/**
[bench]
reason = "The control for fanout_multi_ordered: the same multi-source top-2-by-id question ordered descending, which no forward scan order can satisfy, so every source's full adjacency is scanned and sorted (still through the concurrent fan-out). The gap between this and fanout_multi_ordered is what the per-source ordered cap saves on multi-source batches."
run = true
rebuild = false
datasets = { dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person->(SELECT id FROM knows ORDER BY id DESC LIMIT 2).id FROM person:1..=500;
