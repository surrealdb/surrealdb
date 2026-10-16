/**
[env]
imports = ["bench/util/records-100000-unique-int.surql"]

[bench]
reason = "The largest value of a uniquely indexed int field, read in value position beside each row of an enclosing statement"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id, (SELECT VALUE math::max(integer) FROM record GROUP ALL)[0] AS m FROM record LIMIT 100;
