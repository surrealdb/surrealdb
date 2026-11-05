/**
[env]
imports = ["bench/util/records-seq-100000.surql"]

[bench]
reason = "Filter a wide record-id window by another field, large enough to split into morsels"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE id >= record:10000 AND id < record:90000 AND age > 50;
