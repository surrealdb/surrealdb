/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "Read 100,000 records whole: an output no clause orders, which the merge of a parallel scan takes as its morsels finish"
run = true
rebuild = false
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

SELECT * FROM record;
