/**
[bench]
reason = "A COMPUTED field whose WHERE compares an indexed field to a field of its record, projected on one record: the record is read through one lookup."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE found FROM ONLY rt_cdoc:one;
