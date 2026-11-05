/**
[env]
imports = ["bench/util/create-range-schema.surql"]

[bench]
reason = "Create 100,000 records over a range of ids, the fields of which are computed from the id: the case where the compute half of a CREATE runs apart from its writes"
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
