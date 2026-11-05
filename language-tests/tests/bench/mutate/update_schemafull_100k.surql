/**
[env]
imports = ["bench/util/records-100000-schemafull.surql"]

[bench]
reason = "Update every one of 100,000 records of a SCHEMAFULL table with an expression of the record alone, returning nothing"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

UPDATE record SET name = name + ' hi!', score = score * 2 RETURN NONE;
