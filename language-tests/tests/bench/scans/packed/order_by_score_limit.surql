/**
[bench]
reason = "Top 100 records by score: the top-k probe refuses a packed row whose score cannot enter the sort from its columns, before the row is decoded"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemafull = "bench/util/records-10000-schemafull.surql", mixed = "bench/util/records-10000-schemafull-mixed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record ORDER BY score DESC LIMIT 100;
