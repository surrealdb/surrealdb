/**
[bench]
reason = "A range over one index ordered by another under a LIMIT: the index that delivers the order is walked until ten rows pass the range, where the range's rows would all be read and sorted."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql", typed = "bench/util/records-10000-rule-planner-typed.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT id, created_at FROM record WHERE age > 10 ORDER BY created_at DESC LIMIT 10;
