/**
[bench]
reason = "Low-degree fan-out across 10000 source records (8 edges each) — the inline adjacency cache at one hundred times the source count of fanout_inline_cache, where the per-(source, direction) cursor cost the cache eliminates is multiplied by every source in the batch. The `cached` dataset carries `INLINE EDGES 16` with caches materialised at load; `uncached` is the same graph without the cap."
run = true
rebuild = false
datasets = { cached = "bench/util/graph-fanout-10000-cached.surql", uncached = "bench/util/graph-fanout-10000-uncached.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1..=10000;
