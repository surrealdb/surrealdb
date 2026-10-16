/**
[env]
imports = ["bench/util/references-100000.surql"]

[bench]
reason = "FETCH a single record-link field for every comment of 100000"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "30s"

[test]
run = false
*/

SELECT * FROM comment FETCH author;
