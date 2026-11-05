/**
[bench]
reason = "Low-degree fan-out across 100 source records (8 edges each): the inline adjacency cache's target case, where an uncached traversal pays a cursor per (source, direction) and a cached one answers the whole batch from one multi-get. The `cached` dataset carries `INLINE EDGES 16` with caches materialised at load; `uncached` is the same graph without the cap."
run = true
rebuild = false
datasets = { cached = "bench/util/graph-fanout-1000-cached.surql", uncached = "bench/util/graph-fanout-1000-uncached.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1..=100;
