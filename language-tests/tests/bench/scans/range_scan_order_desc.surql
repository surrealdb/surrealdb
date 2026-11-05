/**
[env]
imports = ["bench/util/records-seq-10000.surql"]

[bench]
reason = "Scan a contiguous mid-keyspace record-id range and sort by score"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

-- 1000 records starting at the 60th percentile of the keyspace, sorted by score.
SELECT * FROM record:6000..7000 ORDER BY score DESC;
