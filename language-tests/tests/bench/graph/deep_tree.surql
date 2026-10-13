/**
[bench]
reason = "A 9-hop bounded recursion collecting the full 29K-node frontier of a ternary tree, classic vs LIGHTWEIGHT — depth-dominated traversal where every hop re-enters the adjacency layer, three hops past the deepest existing recursion bench."
run = true
rebuild = false
datasets = { classic = "bench/util/graph-deep-classic.surql", lightweight = "bench/util/graph-deep-lightweight.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

person:1.{..9+collect}->knows->person;
