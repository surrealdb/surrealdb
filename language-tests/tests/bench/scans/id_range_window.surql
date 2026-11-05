/**
[env]
imports = ["bench/util/records-seq-100000.surql"]

[bench]
reason = "Select a window of record ids written as a predicate (compare range_scan_id, which writes it as a range source)"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE id >= record:60000 AND id < record:61000;
