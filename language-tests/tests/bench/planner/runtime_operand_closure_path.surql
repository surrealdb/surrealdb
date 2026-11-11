/**
[bench]
reason = "A closure argument's field keys a one-column index: one lookup, flat in the table's size."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

array::map([{ k: 4321 }], |$o| (SELECT VALUE id FROM item WHERE k = $o.k));
