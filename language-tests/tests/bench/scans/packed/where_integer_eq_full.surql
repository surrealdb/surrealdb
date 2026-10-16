/**
[bench]
reason = "Select the records matching an integer equality: the filter reads the field in place and only the survivors are decoded"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemafull = "bench/util/records-10000-schemafull.surql", mixed = "bench/util/records-10000-schemafull-mixed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record WHERE number = 21;
