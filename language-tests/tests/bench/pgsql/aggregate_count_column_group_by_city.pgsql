/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "PostgreSQL grouped COUNT(column) over the 100k-row table: nine groups counting the rows whose column holds a value, ordered by the group"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "15s"

[test]
run = false
*/

SELECT city, count(score) AS scored, count(age) AS aged FROM record GROUP BY city ORDER BY city;
