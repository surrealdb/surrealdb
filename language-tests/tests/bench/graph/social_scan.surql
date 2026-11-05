/**
[bench]
reason = "Counting a 100000-edge relation table stored classic vs LIGHTWEIGHT: the classic side scans edge records, the lightweight side has no records and enumerates the IN tables' adjacency subspaces instead — the record-less table-scan path lightweight relations fall back to."
run = true
rebuild = false
datasets = { classic = "bench/util/graph-social-classic.surql", lightweight = "bench/util/graph-social-lightweight.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM knows GROUP ALL;
