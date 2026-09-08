/**
[bench]
reason = "The control for hub_100k_ordered: the same top-10-by-id question ordered descending, which no forward scan order can satisfy, so every one of the 100000 edges is scanned and sorted. The gap between this and hub_100k_ordered is what the ordered-limit scan cap saves."
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(SELECT id FROM knows ORDER BY id DESC LIMIT 10).id FROM person:1;
