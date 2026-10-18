/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Delete every one of 100,000 records, returning nothing"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

DELETE record RETURN NONE;
