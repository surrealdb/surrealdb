/**
[bench]
reason = "The ~1%-selectivity hub predicate when every payload predates the INLINE definition: all 20000 edges fall back to their records, which are prefetched in bulk per scan batch — this pins the fallback path's cost against the payload-served and record-path twins in `filtered_inline`."
run = true
rebuild = false
datasets = { stale = "bench/util/graph-filtered-stale.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(knows WHERE score > 989) FROM person:1;
