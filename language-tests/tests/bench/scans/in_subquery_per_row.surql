/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Filter records by membership in a subquery's values: not an existence read, a regression guard"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE number IN (SELECT VALUE number FROM record WHERE status = 'archived') LIMIT 100;
