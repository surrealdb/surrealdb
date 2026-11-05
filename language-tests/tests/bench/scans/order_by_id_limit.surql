/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "The first 100 records by `id` over 100000, which key order delivers"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record ORDER BY id LIMIT 100;
