/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "PostgreSQL grouped aggregate over the 100k-row table: nine groups folding a count, a mean and a maximum, ordered by the group"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "15s"

[test]
run = false
*/

SELECT city, count(*) AS n, avg(score) AS mean, max(age) AS oldest FROM record GROUP BY city ORDER BY city;
