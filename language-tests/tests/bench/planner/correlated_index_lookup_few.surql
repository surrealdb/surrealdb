/**
[bench]
reason = "A statement joined to fifty rows by an indexed key: each row looks its key up in the index, where a hashed read would read the inner table whole once."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT id, (SELECT VALUE score FROM record WHERE number = $parent.n) AS scores FROM person LIMIT 50;
