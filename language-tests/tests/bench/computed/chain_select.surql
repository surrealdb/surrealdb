/**
[bench]
reason = "Selecting the tail of a three-deep COMPUTED-field chain, against the same expression inlined into a single COMPUTED field — identical arithmetic and identical values per row, so the delta is the cost of deriving a dependency chain field by field"
run = true
rebuild = false
datasets = { chain = "bench/util/records-10000-computed-chain.surql", flat = "bench/util/records-10000-computed-flat.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT rank3 FROM record;
