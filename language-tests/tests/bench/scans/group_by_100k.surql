/**
[bench]
reason = "Group the records of a table of 100000 records; the aggregate reads decoded rows, so the scan does not read ahead of it"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-100000.surql" }
warmup = "2s"
sample-size = 20
measurement-time = "30s"

[test]
run = false
*/

SELECT city, count() FROM record GROUP BY city;
