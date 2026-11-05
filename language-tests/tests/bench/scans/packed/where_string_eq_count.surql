/**
[bench]
reason = "Count records matching a string equality: the grouped body runs on each stored row, reading the field from a packed row's columns"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemafull = "bench/util/records-10000-schemafull.surql", mixed = "bench/util/records-10000-schemafull-mixed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE city = 'Tokyo' GROUP ALL;
