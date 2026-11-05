/**
[bench]
reason = "Count records within an integer range most records fall in"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql", typed = "bench/util/records-10000-indexed-typed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE number >= 1 GROUP ALL;
