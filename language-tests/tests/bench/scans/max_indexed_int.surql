/**
[env]
imports = ["bench/util/records-100000-unique-int.surql"]

[bench]
reason = "The largest value of a uniquely indexed int field"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT math::max(integer) AS m FROM record GROUP ALL;
