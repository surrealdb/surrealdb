/**
[bench]
reason = "Project a field of the records whose integer is in a set of forty, over a table with a computed field: the pre-decode filter refuses a packed row whose integer is outside the set from its columns, and the scan decodes the rest"
run = true
rebuild = false
datasets = { schemaless = "bench/util/records-10000.surql", schemaless_computed = "bench/util/records-10000-schemaless-computed.surql", computed = "bench/util/records-10000-schemafull-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT name FROM record WHERE number IN [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40];
