/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Sort all 100,000 records by score, no limit: a sort that keeps every row, which the morsels' sorted runs merge"
run = true
rebuild = false
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

SELECT id, score FROM record ORDER BY score;
