/**
[bench]
reason = "The degree of a single 100000-degree vertex on plain per-edge keys: the fast path counts adjacency keys without decoding or materializing them, against the evaluator's collect-then-count. Compare with hub_100k_degree_folded, where chunk-header sums answer the same question in O(chunks)."
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(->knows) AS degree FROM ONLY person:1;
