/**
[env]
imports = ["bench/util/records-10000-computed.surql"]

[bench]
reason = "Filter over a read-time COMPUTED field: every row is decoded, the field derived and coerced into the document, and only then is the predicate judged. Paired with `computed/where_inline`, which evaluates the same body directly in the predicate over the same rows"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE bucket = 5;
