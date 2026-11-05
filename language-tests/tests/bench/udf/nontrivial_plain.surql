/**
[bench]
reason = "A user-defined function whose body is not inlined, called once per row of 10k."
run = true
rebuild = false
datasets = { udf = "bench/util/records-10000-udf.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE [fn::nontrivial(n), IF false THEN 0 END] FROM record;
