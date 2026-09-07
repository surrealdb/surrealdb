/**
[bench]
reason = "An indexed predicate AND a graph containment against a 500-in-edge hub, over 200000 rows. Fused, the traversal branch is one adjacency band scan from the hub plus a batched doc-ID lookup, intersected before any record is fetched; unfused, every candidate row opens its own graph cursor. Compare medians against main."
run = true
rebuild = false
datasets = { semijoin = "bench/util/planner-semijoin-200k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:hub GROUP ALL;
