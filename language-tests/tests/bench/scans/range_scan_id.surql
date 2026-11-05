/**
[env]
imports = ["bench/util/records-seq-10000.surql"]

[bench]
reason = "Scan a contiguous mid-keyspace record-id range (ids only)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

-- 1000 records starting at the 60th percentile of the keyspace.
SELECT id FROM record:6000..7000;
