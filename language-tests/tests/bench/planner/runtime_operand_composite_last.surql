/**
[bench]
reason = "A function argument in the last column of a three-column index, literals in the others: one lookup over every column, not a read of the literal prefix."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

fn::rt_pair_last(4321);
