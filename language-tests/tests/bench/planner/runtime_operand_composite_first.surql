/**
[bench]
reason = "A function argument in the first column of a three-column index, literals in the others: one lookup over every column."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

fn::rt_pair_first({ a: 1 });
