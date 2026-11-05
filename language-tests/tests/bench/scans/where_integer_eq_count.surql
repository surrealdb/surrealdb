/**
[bench]
reason = "Count records matching an integer equality"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql", typed = "bench/util/records-10000-indexed-typed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE number = 21 GROUP ALL;
