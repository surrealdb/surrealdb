/**
[bench]
reason = "The nontrivial_plain statement holding a call that may write the catalog: the function is called per row through its call unit, as the catalog stands at the call."
run = true
rebuild = false
datasets = { udf = "bench/util/records-10000-udf.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE [fn::nontrivial(n), IF false THEN function() { return 0 } END] FROM record;
