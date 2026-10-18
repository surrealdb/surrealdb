/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Sum the score of 100,000 records: a float fold of one group, which the merge folds in the order of the serial run"
run = true
rebuild = false
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

SELECT math::sum(score) FROM record GROUP ALL;
