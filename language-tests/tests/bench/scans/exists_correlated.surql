/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Filter records on whether a correlated subquery has a row"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE (SELECT id FROM record WHERE number > $parent.number) LIMIT 100;
