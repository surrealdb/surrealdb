/**
[bench]
reason = "Everything-on filtered traversal: the filtered_100k inline dataset folded into packed blocks with numeric doc-id entries, so the payload-resident predicate is evaluated while streaming block chunks. Compare medians against filtered_100k (inline = payload over delta keys, plain = record fetches) for the composed effect of inline props × blocks × numeric ids."
run = true
rebuild = false
graph-numeric-ids = true
datasets = { inline = "bench/util/graph-filtered-100k-inline.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(knows WHERE score > 9899) FROM person:1;
