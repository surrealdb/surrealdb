/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Every record id of 10000 in `id` order, which key order delivers"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record ORDER BY id;
