/**
[bench]
reason = "PostgreSQL join of two thousand left rows to an indexed right key: the first lookups hand the rest of the left rows to the hash join."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT p.id, r.score FROM person p JOIN record r ON p.n = r.number;
