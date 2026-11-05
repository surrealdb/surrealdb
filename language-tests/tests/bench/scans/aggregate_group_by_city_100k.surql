/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Group 100,000 records by city with a count"
run = true
rebuild = false
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

SELECT city, count() FROM record GROUP BY city;
