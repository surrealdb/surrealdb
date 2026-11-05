/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "As update_all_100k, returning nothing: the statement's output is not built"
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

UPDATE record SET name = name + ' hi!' RETURN NONE;
