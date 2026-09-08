/**
[bench]
reason = "Composed social traversal: the social_traverse statement over a LIGHTWEIGHT relation folded into packed blocks with numeric doc-id entries — lightweight edges × blocks × numeric ids, with no inline cache so the timed statement actually streams block chunks. Compare medians against social_traverse's two sides."
run = true
rebuild = false
graph-numeric-ids = true
datasets = { lightweight_folded = "bench/util/graph-social-lightweight-folded.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->knows->person FROM person:1..=2000;
