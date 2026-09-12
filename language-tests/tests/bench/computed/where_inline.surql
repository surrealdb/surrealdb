/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "The `computed/where_computed` filter with the COMPUTED field's body written inline in the predicate instead — same rows, same arithmetic, no field definitions on the table, so the pair separates the cost of the materialisation machinery from the cost of the expression"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE math::floor(score / 10) = 5;
