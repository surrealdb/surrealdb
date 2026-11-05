/**
[bench]
reason = "Project a field of nearly every record, over a table with a computed field: the predicate keeps nearly every row, so the scan checks no packed row for its pre-decode filter and decodes each one whole"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemaless_computed = "bench/util/records-10000-schemaless-computed.surql", computed = "bench/util/records-10000-schemafull-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT name FROM record WHERE number != 21;
