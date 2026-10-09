/**
[bench]
reason = "PostgreSQL join of fifty left rows to an indexed right key: each left row looks its key up in the index, where the hash join would read the right table whole."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT p.id, r.score FROM (SELECT id, n FROM person LIMIT 50) p JOIN record r ON p.n = r.number;
