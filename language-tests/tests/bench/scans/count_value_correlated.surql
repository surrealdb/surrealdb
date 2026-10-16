/**
[env]
imports = ["bench/util/records-100000-indexed-typed.surql"]

[bench]
reason = "Count the rows of a table that share a field with each row of an enclosing statement"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id, (SELECT count() FROM record WHERE number = $parent.number GROUP ALL)[0].count AS c FROM record LIMIT 100;
