/**
[bench]
reason = "The deep_tree recursion after the classic tree is folded into packed blocks with numeric doc-id entries: 9 hops of frontier expansion where every scope is block-resident, measuring the fold's effect on depth-dominated (many small scopes) rather than degree-dominated traversal. Compare medians against deep_tree's classic side."
run = true
rebuild = false
graph-numeric-ids = true
datasets = { classic = "bench/util/graph-deep-classic.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

person:1.{..9+collect}->knows->person;
