/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Update every one of 100,000 records with an expression of the record alone: the compute half of the write runs apart from the writes"
run = true
rebuild = true
rebuild-file-backend = true
# One iteration, re-import included, outlasts quick mode's cap (`QUICK_MAX_TIME`).
quick = false
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

UPDATE record SET name = name + ' hi!';
