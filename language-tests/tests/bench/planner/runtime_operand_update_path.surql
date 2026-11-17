/**
[bench]
reason = "An UPDATE whose WHERE compares an indexed field to a function argument's field: the subjects are read through one lookup."
run = true
rebuild = true
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "10s"

[test]
run = false
*/

fn::rt_bump_path({ k: 4321 });
