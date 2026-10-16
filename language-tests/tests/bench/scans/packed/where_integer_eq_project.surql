/**
[bench]
reason = "Project two fields of the records matching an integer equality: the table has a computed field, so the scan decodes its rows and the pre-decode filter refuses a packed row from its columns first"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemaless_computed = "bench/util/records-10000-schemaless-computed.surql", computed = "bench/util/records-10000-schemafull-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT name, age FROM record WHERE number = 21;
