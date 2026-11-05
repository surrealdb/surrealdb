/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Selective projection over a table with four COMPUTED fields the query never names, where a field permission's dependencies are statically resolvable versus written with a parameter — an unresolvable guard cannot prove any computed field unnecessary, so all four are derived per row; both arms return the same documents"
run = true
rebuild = false
datasets = { static = "bench/util/records-10000-computed-deps-static.surql", opaque = "bench/util/records-10000-computed-deps-opaque.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT name FROM record;
