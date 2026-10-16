/**
[bench]
reason = "Count records matching a string equality: the scan's pre-decode filter refuses a packed row from its columns, and the grouped body runs on each record that remains, decoded whole"
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
