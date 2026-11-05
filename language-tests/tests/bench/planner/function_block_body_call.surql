/**
[bench]
reason = "A user-defined function whose body is a LET and a RETURN, called up to four times per row of a 10,000-record scan: each call runs the body as a unit of its own."
run = true
rebuild = false
datasets = { functions = "bench/util/records-10000-functions.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT count() FROM record WHERE fn::over_block(score) OR fn::over_block(age) OR fn::over_block(tier) OR fn::over_block(score - 50) GROUP ALL;
