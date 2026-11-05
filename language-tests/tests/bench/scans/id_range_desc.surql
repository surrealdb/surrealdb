/**
[env]
imports = ["bench/util/records-seq-100000.surql"]

[bench]
reason = "Page backwards from a record-id bound"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE id < record:40000 ORDER BY id DESC LIMIT 100;
