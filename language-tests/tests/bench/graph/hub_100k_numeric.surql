/**
[bench]
reason = "The hub_100k traversal over blocks whose entries are numeric doc-ids rather than record keys: adds the doc-id resolve step to the folded read, in exchange for the smaller chunks. Compare medians against hub_100k_folded (resolve overhead) and hub_100k_plain (net win)."
run = true
rebuild = false
graph-numeric-ids = true
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1;
