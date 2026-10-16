/**
[bench]
reason = "Count records matching an equality on a deeply nested object field: the scan's pre-decode filter reads the nested path of a packed row from its columns and refuses it, and the grouped body runs on each record that remains, decoded whole"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemafull = "bench/util/records-10000-schemafull.surql", mixed = "bench/util/records-10000-schemafull-mixed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE geography.region.details.continent = 'Asia' GROUP ALL;
