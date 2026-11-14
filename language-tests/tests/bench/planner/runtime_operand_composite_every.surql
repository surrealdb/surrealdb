/**
[bench]
reason = "A function argument's fields in every column of a three-column index: one lookup over every column."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

fn::rt_pair_every({ a: 1, b: 21, k: 4321 });
