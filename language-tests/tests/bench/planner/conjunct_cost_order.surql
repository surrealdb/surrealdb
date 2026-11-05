/**
[bench]
reason = "A type test written before a selective comparison: both are total, so the comparison is judged first and rejects most rows before the call."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT id FROM record WHERE type::is_string(name) AND score > 95;
