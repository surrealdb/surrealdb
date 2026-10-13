/**
[bench]
reason = "A record-disjoint top-level OR: two equality branches on the same single-column index with distinct values (~66k rows each, zero overlap by construction). The worst case for bitmap union fusion — there are no duplicate record fetches for a fused plan to save, so its doc-ID resolve step is pure overhead — which is why the planner declines fusion for this shape and keeps the streaming UnionIndexScan. Guards that decline: compare medians against or_filter_200k's dedupe-heavy best case."
run = true
rebuild = false
datasets = { items = "bench/util/planner-items-200k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM items WHERE category = 'red' OR category = 'blue' GROUP ALL;
