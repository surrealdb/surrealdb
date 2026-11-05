/**
[bench]
reason = "Filter a whole table of 100000 records in the scan, keeping about one record in a thousand"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-100000.surql" }
warmup = "2s"
sample-size = 20
measurement-time = "30s"

[test]
run = false
*/

SELECT * FROM record WHERE score > 99.9;
