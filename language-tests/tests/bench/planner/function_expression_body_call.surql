/**
[bench]
reason = "A user-defined function whose body is one expression, called up to four times per row of a 10,000-record scan: the call inlines the body, the reference a call of a small unit is measured against."
run = true
rebuild = false
datasets = { functions = "bench/util/records-10000-functions.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT count() FROM record WHERE fn::over_expr(score) OR fn::over_expr(age) OR fn::over_expr(tier) OR fn::over_expr(score - 50) GROUP ALL;
