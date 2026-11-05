/**
[bench]
reason = "A statement joined to two thousand rows by an indexed key: the first lookups hand the rest of the run to the probe, which reads the inner table once."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT id, (SELECT VALUE score FROM record WHERE number = $parent.n) AS scores FROM person;
