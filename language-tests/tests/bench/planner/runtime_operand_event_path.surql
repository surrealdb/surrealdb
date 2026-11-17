/**
[bench]
reason = "An event's UPDATE whose WHERE compares an indexed field to a field of `$after`: the subjects are read through one lookup."
run = true
rebuild = true
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "10s"

[test]
run = false
*/

CREATE rt_src SET k = 4321 RETURN NONE;
