/**
[env]
imports = ["bench/util/create-range-schema-indexed.surql"]

[bench]
reason = "Create 100,000 records over a range of ids into a table with an index on a computed field"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

CREATE |record:1..100000| SET tags = ['a', 'b', 'c'] RETURN NONE;
