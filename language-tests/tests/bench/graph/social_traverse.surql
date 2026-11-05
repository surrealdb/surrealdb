/**
[bench]
reason = "A 2000-source, 50-edges-each traversal (100000 edges walked per iteration) over identical topology stored classic vs LIGHTWEIGHT — isolating what per-edge records cost a bulk read, and what edge synthesis costs a record-less one."
run = true
rebuild = false
datasets = { classic = "bench/util/graph-social-classic.surql", lightweight = "bench/util/graph-social-lightweight.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1..=2000;
