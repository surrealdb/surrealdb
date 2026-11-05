/**
[bench]
reason = "A ~1%-selectivity endpoint filter over the 100000-edge hub: the predicate reads only `out`, which the adjacency entry itself carries, so the scan prunes non-matching edges before any record fetch — on a plain relation with no INLINE fields declared. The dataset is feature-neutral, so the same file measures the fetch-everything baseline on main."
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(knows WHERE out > person:99000) FROM person:1;
