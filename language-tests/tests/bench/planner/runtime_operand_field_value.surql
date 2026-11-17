/**
[bench]
reason = "A field's VALUE clause whose WHERE compares an indexed field to the field's `$value`: the record is read through one lookup."
run = true
rebuild = true
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "10s"

[test]
run = false
*/

CREATE rt_doc SET found = 4321 RETURN NONE;
