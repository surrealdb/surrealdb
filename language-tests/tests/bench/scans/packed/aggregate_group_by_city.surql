/**
[bench]
reason = "Group records by a string column with a count and a mean of a float column"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemafull = "bench/util/records-10000-schemafull.surql", mixed = "bench/util/records-10000-schemafull-mixed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT city, count() AS c, math::mean(score) AS m FROM record GROUP BY city;
