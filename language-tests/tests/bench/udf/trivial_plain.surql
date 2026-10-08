/**
[bench]
reason = "A trivial user-defined function called once per row of 10k: inlined into the statement."
run = true
rebuild = false
datasets = { udf = "bench/util/records-10000-udf.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE fn::trivial(n) FROM record;
