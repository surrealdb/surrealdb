/**
[bench]
reason = "trivial_plain with the same array shape as trivial_within, so the two differ only in how the function is called."
run = true
rebuild = false
datasets = { udf = "bench/util/records-10000-udf.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE [fn::trivial(n), IF false THEN 0 END] FROM record;
